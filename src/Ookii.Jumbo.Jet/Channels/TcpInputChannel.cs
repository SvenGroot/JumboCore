// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// Represents the reading end of a TCP channel
/// </summary>
public class TcpInputChannel : InputChannel, IHasMetrics
{
    #region Nested types

    private sealed class TcpChannelConnectionHandler : IDisposable
    {
        private readonly NetworkStream _stream;
        private readonly Socket _socket;
        private readonly byte[] _header = new byte[HeaderSize];
        private readonly TcpInputChannel _channel;

        public TcpChannelConnectionHandler(TcpInputChannel channel, Socket socket)
        {
            _channel = channel;
            _socket = socket;
            _stream = new NetworkStream(socket);
            _stream.WriteTimeout = 30000;
        }

        public NetworkStream Stream
        {
            get { return _stream; }
        }

        public void HandleConnection()
        {
            try
            {
                _stream.BeginRead(_header, 0, HeaderSize, BeginReadCallback, null);
            }
            catch (Exception ex)
            {
                CloseOnError(ex);
                throw; // Transmission failure is a non-recoverable error for a TCP input channel
            }
        }

        public void CloseOnError(Exception ex)
        {
            TrySendError(ex);
            Close();
        }

        private void BeginReadCallback(IAsyncResult ar)
        {
            try
            {
                var bytesRead = _stream.EndRead(ar);
                if (bytesRead != HeaderSize)
                {
                    throw new ChannelException("Bad TCP channel header format.");
                }
                else
                {
                    var flags = (TcpChannelConnectionFlags)_header[0];
                    var sendingTaskNumber = ReadInt32(1);
                    var segmentNumber = ReadInt32(5);

                    if (sendingTaskNumber < 1 || sendingTaskNumber > _channel.InputStage.Root.TaskCount)
                    {
                        throw new ChannelException("Invalid sending task number.");
                    }

                    if (flags.HasFlag(TcpChannelConnectionFlags.KeepAlive))
                    {
                        _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                    }

                    _stream.ReadTimeout = 30000;
                    _channel.HandleSegment(sendingTaskNumber, segmentNumber, flags.HasFlag(TcpChannelConnectionFlags.FinalSegment), this);

                    SendResponse(null); // Send success

                    if (flags.HasFlag(TcpChannelConnectionFlags.KeepAlive) && !flags.HasFlag(TcpChannelConnectionFlags.FinalSegment))
                    {
                        _stream.ReadTimeout = Timeout.Infinite;
                        HandleConnection(); // Read the next header.
                    }
                    else
                    {
                        Close(); // We're done
                    }
                }
            }
            catch (Exception ex)
            {
                CloseOnError(ex);
                throw; // Transmission failure is a non-recoverable error for a TCP input channel
            }
        }

        public void ReadPartitionHeader(out int partition, out int partitionSize)
        {
            var totalBytesRead = 0;
            do
            {
                var bytesRead = _stream.Read(_header, totalBytesRead, PartitionHeaderSize - totalBytesRead);
                if (bytesRead == 0)
                {
                    throw new ChannelException("Invalid segment format.");
                }

                totalBytesRead += bytesRead;
            } while (totalBytesRead < PartitionHeaderSize);

            partition = ReadInt32(0);
            partitionSize = ReadInt32(4);
        }

        private int ReadInt32(int index)
        {
            return _header[index] | _header[index + 1] << 8 | _header[index + 2] << 16 | _header[index + 3] << 24;
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void TrySendError(Exception ex)
        {
            try
            {
                SendResponse(ex);
            }
            catch
            {
            }
        }

        private void SendResponse(Exception? ex)
        {
            if (ex != null)
            {
                using var contentStream = new MemoryStream();
                contentStream.WriteByte(0);
                using var writer = new BinaryWriter(contentStream);
                RpcRemoteException.WriteTo(ex, writer);
                contentStream.WriteTo(_stream);
            }
            else
            {
                _stream.WriteByte(1);
            }
        }

        private void Close()
        {
            _stream.Dispose();
            _socket.Close();
        }

        public void Dispose()
        {
            Close();
            GC.SuppressFinalize(this);
        }
    }

    #endregion

    internal const int HeaderSize = 9; // task number + flags + segment number
    internal const int PartitionHeaderSize = 8; // partition number + size

    private IMultiInputRecordReader? _reader;
    private readonly Type _inputReaderType;
    private TcpListener[]? _listeners;
    private readonly ITcpChannelRecordReader[][] _inputReaders;
    private readonly bool _running = true;

    /// <summary>
    /// Initializes a new instance of the <see cref="TcpInputChannel"/> class.
    /// </summary>
    /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
    /// <param name="inputStage">The input stage that this file channel reads from.</param>
    public TcpInputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
        : base(taskExecution, inputStage)
    {
        ArgumentNullException.ThrowIfNull(inputStage);
        _inputReaderType = typeof(TcpChannelRecordReader<>).MakeGenericType(InputRecordType);
        _inputReaders = new ITcpChannelRecordReader[inputStage.Root.TaskCount][];
    }

    /// <summary>
    /// Gets a value indicating whether the input channel uses memory storage to store inputs.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
    /// </value>
    public override bool UsesMemoryStorage
    {
        get { return false; }
    }

    /// <summary>
    /// Gets the current memory storage usage level.
    /// </summary>
    /// <value>The memory storage usage level, between 0 and 1.</value>
    /// <remarks>
    /// 	<para>
    /// The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
    /// </para>
    /// 	<para>
    /// If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
    /// the actual level.
    /// </para>
    /// </remarks>
    public override float MemoryStorageLevel
    {
        get { return 0; }
    }

    /// <summary>
    /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
    /// </summary>
    /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
    public override Ookii.Jumbo.IO.IRecordReader CreateRecordReader()
    {
        _reader = CreateChannelRecordReader();

        var addresses = TcpServer.GetDefaultListenerAddresses(TaskExecution.JetClient.Configuration.TaskServer.ListenIPv4AndIPv6);

        _listeners = new TcpListener[addresses.Length];

        var port = 0;
        for (var x = 0; x < addresses.Length; ++x)
        {
            var listener = new TcpListener(addresses[x], port);
            _listeners[x] = listener;
            listener.Start();
            if (port == 0)
            {
                port = ((IPEndPoint)listener.LocalEndpoint).Port;
            }

            listener.BeginAcceptSocket(BeginAcceptCallback, listener);
        }
        TaskExecution.Umbilical.RegisterTcpChannelPort(TaskExecution.Context.JobId, TaskExecution.Context.TaskAttemptId, port);

        return _reader;
    }

    /// <summary>
    /// Assigns additional partitions to this input channel.
    /// </summary>
    /// <param name="additionalPartitions">The additional partitions.</param>
    /// <remarks>
    /// <para>
    ///   The TCP input channel does not support this method, and will always throw a <see cref="NotSupportedException"/>.
    /// </para>
    /// </remarks>
    public override void AssignAdditionalPartitions(IList<int> additionalPartitions)
    {
        throw new NotSupportedException();
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
    private void BeginAcceptCallback(IAsyncResult ar)
    {
        var listener = (TcpListener)ar.AsyncState!;
        if (_running)
        {
            listener.BeginAcceptSocket(BeginAcceptCallback, listener);
        }

        TcpChannelConnectionHandler? handler = null;
        Socket? socket = null;
        try
        {
            socket = listener.EndAcceptSocket(ar);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, 3));
            socket.NoDelay = true;
            handler = new TcpChannelConnectionHandler(this, socket);
            handler.HandleConnection();
        }
        catch (Exception ex)
        {
            if (handler != null)
            {
                handler.CloseOnError(ex);
            }
            else if (socket != null)
            {
                socket.Close();
            }
        }
    }

    private void HandleSegment(int taskNumber, int segmentNumber, bool finalSegment, TcpChannelConnectionHandler handler)
    {
        ITcpChannelRecordReader[] readers;
        lock (_inputReaders)
        {
            readers = _inputReaders[taskNumber - 1];
            if (readers == null)
            {
                var inputs = new RecordInput[ActivePartitions.Count];
                readers = new ITcpChannelRecordReader[ActivePartitions.Count];
                for (var x = 0; x < readers.Length; ++x)
                {
                    var reader = (ITcpChannelRecordReader)JetActivator.CreateInstance(_inputReaderType, TaskExecution, TaskExecution.Context.StageConfiguration.AllowRecordReuse);
                    readers[x] = reader;
                    inputs[x] = new ReaderRecordInput((IRecordReader)reader, true);
                }
                _inputReaders[taskNumber - 1] = readers;
                _reader!.AddInput(inputs);
            }
        }

        lock (readers)
        {
            for (var x = 0; x < readers.Length; ++x)
            {
                var reader = readers[x];
                handler.ReadPartitionHeader(out var partition, out var partitionSize);
                if (partition != ActivePartitions[x])
                {
                    throw new ChannelException(string.Format(CultureInfo.InvariantCulture, "Received partition {0}, excepted {1}.", partition, ActivePartitions[x]));
                }

                reader.AddSegment(partitionSize, segmentNumber, handler.Stream);
                if (finalSegment)
                {
                    reader.CompleteAdding();
                }
            }
        }
    }

    /// <summary>
    /// Gets the number of bytes read from the local disk.
    /// </summary>
    /// <value>The local bytes read.</value>
    public long LocalBytesRead
    {
        get { return 0L; }
    }

    /// <summary>
    /// Gets the number of bytes written to the local disk.
    /// </summary>
    /// <value>The local bytes written.</value>
    public long LocalBytesWritten
    {
        get { return 0L; }
    }

    /// <summary>
    /// Gets the number of bytes read over the network.
    /// </summary>
    /// <value>The network bytes read.</value>
    /// <remarks>Only channels should normally use this property.</remarks>
    public long NetworkBytesRead
    {
        get { return _reader == null ? 0L : _reader.BytesRead; }
    }

    /// <summary>
    /// Gets the number of bytes written over the network.
    /// </summary>
    /// <value>The network bytes written.</value>
    /// <remarks>Only channels should normally use this property.</remarks>
    public long NetworkBytesWritten
    {
        get { return 0L; }
    }
}
