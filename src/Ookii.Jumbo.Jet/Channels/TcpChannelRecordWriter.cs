// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Ookii.Jumbo.IO;

#pragma warning disable SYSLIB0011 // BinaryFormatter is deprecated.

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class TcpChannelRecordWriter<T> : SpillRecordWriter<T>
    {
        #region Nested types

        private struct TaskConnectionInfo : IDisposable
        {
            public string HostName { get; set; }
            public int Port { get; set; }
            public int[] Partitions { get; set; }
            public TcpClient Client { get; set; }
            public WriteBufferedStream ClientStream { get; set; }

            public void Dispose()
            {
                if (ClientStream != null)
                    ClientStream.Dispose();
                if (Client != null)
                    ((IDisposable)Client).Dispose();
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpOutputChannel));
        private const int _retryDelay = 2000;

        private readonly TaskId[] _outputIds;
        private readonly bool _reuseConnections;
        private readonly byte[] _header = new byte[TcpInputChannel.HeaderSize + TcpInputChannel.PartitionHeaderSize];
        private readonly TaskConnectionInfo[] _taskConnections;
        private readonly TaskExecutionUtility _taskExecution;
        private bool _hasFinalSpill;
        private bool _disposed;
        private long _headerBytesWritten;

        public TcpChannelRecordWriter(TaskExecutionUtility taskExecution, bool reuseConnections, IPartitioner<T> partitioner, int bufferSize, int limit)
            : base(partitioner, bufferSize, limit, SpillRecordWriterOptions.AllowRecordWrapping | SpillRecordWriterOptions.AllowMultiRecordIndexEntries)
        {
            var outputStage = taskExecution.Context.JobConfiguration.GetStage(taskExecution.Context.StageConfiguration.OutputChannel.OutputStage);
            _outputIds = new TaskId[outputStage.TaskCount]; // We need this to be task based, not partition based.
            for (var x = 0; x < _outputIds.Length; ++x)
                _outputIds[x] = new TaskId(outputStage.StageId, x + 1);
            _taskConnections = new TaskConnectionInfo[_outputIds.Length];
            _taskExecution = taskExecution;
            _reuseConnections = reuseConnections;
            if (reuseConnections)
                _header[0] = (byte)TcpChannelConnectionFlags.KeepAlive;
            WriteInt32ToHeader(1, taskExecution.RootTask.Context.TaskAttemptId.TaskId.TaskNumber);
        }

        public override long BytesWritten
        {
            get
            {
                return base.BytesWritten + Interlocked.Read(ref _headerBytesWritten);
            }
        }

        protected override void SpillOutput(bool finalSpill)
        {
            SendSegments(finalSpill, true);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (!_disposed)
            {
                _disposed = true;
                if (!_hasFinalSpill && !ErrorOccurred)
                {
                    SendSegments(true, false); // Send empty partitions to all tasks to signify the end of writing
                }

                if (disposing)
                {
                    for (var x = 0; x < _taskConnections.Length; ++x)
                        _taskConnections[x].Dispose();
                }
            }
        }

        private void SendSegments(bool finalSpill, bool sendData)
        {
            WriteInt32ToHeader(5, sendData ? SpillCount : (SpillCount + 1));
            if (finalSpill)
            {
                _hasFinalSpill = true;
                _header[0] |= (byte)TcpChannelConnectionFlags.FinalSegment;
            }

            for (var taskIndex = 0; taskIndex < _outputIds.Length; ++taskIndex)
            {
                SendSegmentToTask(sendData, taskIndex);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership may be handed off to cached connection.")]
        private void SendSegmentToTask(bool sendData, int taskIndex)
        {
            var disposeStream = false;
            var stream = _taskConnections[taskIndex].ClientStream;
            TcpClient client = null;
            try
            {
                var partitions = _taskConnections[taskIndex].Partitions;
                if (partitions == null)
                {
                    partitions = _taskExecution.Context.StageConfiguration.OutputChannel.PartitionsPerTask <= 1 ? new[] { _outputIds[taskIndex].TaskNumber } : _taskExecution.JobServerTaskClient.GetPartitionsForTask(_taskExecution.Context.JobId, _outputIds[taskIndex]);
                    _taskConnections[taskIndex].Partitions = partitions;
                }

                var sentHeader = false;
                // Always send all partitions, even if they're empty
                foreach (var partition in partitions)
                {
                    var size = sendData ? SpillDataSizeForPartition(partition - 1) : 0;
                    if (stream == null)
                    {
                        disposeStream = true;
                        client = ConnectToTask(taskIndex);
                        stream = new WriteBufferedStream(client.GetStream());
                        if (_reuseConnections)
                        {
                            _taskConnections[taskIndex].Client = client;
                            _taskConnections[taskIndex].ClientStream = stream;
                            disposeStream = false;
                        }
                    }

                    WriteInt32ToHeader(TcpInputChannel.HeaderSize, partition);
                    WriteInt32ToHeader(TcpInputChannel.HeaderSize + 4, size);
                    if (!sentHeader)
                    {
                        // Send header + partition header
                        stream.Write(_header, 0, _header.Length);
                        sentHeader = true;
                        Interlocked.Add(ref _headerBytesWritten, _header.Length);
                    }
                    else
                    {
                        // Send partition header only
                        stream.Write(_header, TcpInputChannel.HeaderSize, TcpInputChannel.PartitionHeaderSize);
                        Interlocked.Add(ref _headerBytesWritten, TcpInputChannel.PartitionHeaderSize);
                    }
                    if (sendData && size > 0)
                        WritePartition(partition - 1, stream);

                }
                stream.Flush();
                CheckResult(stream, taskIndex);
            }
            finally
            {
                if (disposeStream)
                {
                    if (stream != null)
                        stream.Dispose();
                    if (client != null)
                        ((IDisposable)client).Dispose();
                }
            }
        }

        private void CheckResult(WriteBufferedStream stream, int taskIndex)
        {
            var result = stream.ReadByte();
            if (result == -1)
                throw new ChannelException(string.Format(CultureInfo.InvariantCulture, "Task {0} did not send a status result.", _outputIds[taskIndex]));
            else if (result == 0)
            {
                var formatter = new BinaryFormatter();
                var ex = (Exception)formatter.Deserialize(stream);
                throw new ChannelException(string.Format(CultureInfo.InvariantCulture, "Task {0} encountered an exception reading channel data.", _outputIds[taskIndex]), ex);
            }
        }

        private void WriteInt32ToHeader(int index, int value)
        {
            _header[index] = (byte)(value & 0xFF);
            _header[index + 1] = (byte)((value >> 8) & 0xFF);
            _header[index + 2] = (byte)((value >> 16) & 0xFF);
            _header[index + 3] = (byte)((value >> 24) & 0xFF);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        private TcpClient ConnectToTask(int taskIndex)
        {
            if (_taskConnections[taskIndex].HostName == null)
            {
                var taskId = _outputIds[taskIndex];
                ServerAddress taskServer;
                do
                {
                    taskServer = _taskExecution.JetClient.JobServer.GetTaskServerForTask(_taskExecution.Context.JobId, taskId.ToString());
                    if (taskServer == null)
                    {
                        _log.DebugFormat("Task {0} is not yet assigned to a server, waiting for retry...", taskId);
                        Thread.Sleep(_retryDelay);
                    }
                } while (taskServer == null);

                _log.InfoFormat("Task {0} is running on task server {1}", taskId, taskServer);

                var taskServerClient = JetClient.CreateTaskServerClient(taskServer);
                int port;
                do
                {
                    // Since a task failure fails the job with the TCP channel, the attempt number will always be 1.
                    port = taskServerClient.GetTcpChannelPort(_taskExecution.Context.JobId, new TaskAttemptId(taskId, 1));
                    if (port == 0)
                    {
                        _log.DebugFormat("Task {0} has not yet registered a port number, waiting for retry...", taskId);
                        Thread.Sleep(_retryDelay);
                    }
                } while (port == 0);

                _taskConnections[taskIndex].HostName = taskServer.HostName;
                _taskConnections[taskIndex].Port = port;
            }

            _log.DebugFormat("Connecting to task {0} at TCP channel server {1}:{2}", _outputIds[taskIndex], _taskConnections[taskIndex].HostName, _taskConnections[taskIndex].Port);

            var client = new TcpClient(_taskConnections[taskIndex].HostName, _taskConnections[taskIndex].Port);
            if (_reuseConnections)
                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            client.Client.NoDelay = true;
            return client;
        }
    }
}
