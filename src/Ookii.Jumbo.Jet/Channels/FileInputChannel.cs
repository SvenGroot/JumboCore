// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// Represents the reading end of a file channel.
/// </summary>
[AdditionalProgressCounter("Shuffle")]
public class FileInputChannel : InputChannel, IDisposable, IHasAdditionalProgress, IHasMetrics
{
    #region Nested types

    private sealed class InputServer
    {
        private static readonly string _localHostName = Dns.GetHostName();
        private readonly ServerAddress _taskServer;
        private readonly int _fileServerPort;
        private readonly List<CompletedTask> _completedTasks = new List<CompletedTask>();
        private readonly List<CompletedTask> _tasksToDownload = new List<CompletedTask>(); // may only be accessed by the download thread

        public InputServer(ServerAddress taskServer, int fileServerPort)
        {
            _taskServer = taskServer;
            _fileServerPort = fileServerPort;
        }

        public ServerAddress TaskServer
        {
            get { return _taskServer; }
        }

        public int FileServerPort
        {
            get { return _fileServerPort; }
        }

        public bool IsLocal
        {
            get
            {
                return _taskServer.HostName == _localHostName;
            }
        }

        /// <summary>
        /// NOTE: Only call inside _orderedServers lock
        /// </summary>
        /// <param name="task"></param>
        public void AddCompletedTask(CompletedTask task)
        {
            _completedTasks.Add(task);
        }

        /// <summary>
        /// NOTE: Only use from download thread.
        /// </summary>
        public IEnumerable<CompletedTask> TasksToDownload
        {
            get
            {
                return _tasksToDownload;
            }
        }

        /// <summary>
        /// NOTE: Only use from download thread.
        /// </summary>
        public int TasksToDownloadCount
        {
            get
            {
                return _tasksToDownload.Count;
            }
        }

        /// <summary>
        /// NOTE: Only call from download thread and inside _orderedServers lock.
        /// </summary>
        /// <returns></returns>
        public bool UpdateTasksToDownload()
        {
            _tasksToDownload.AddRange(_completedTasks);
            _completedTasks.Clear();

            return _tasksToDownload.Count > 0;
        }

        /// <summary>
        /// NOTE: Only call from download thread.
        /// </summary>
        public void NotifyDownloadSuccess(int count)
        {
            if (count == _tasksToDownload.Count)
            {
                _tasksToDownload.Clear();
            }
            else
            {
                _tasksToDownload.RemoveRange(0, count);
            }
        }
    }

    private sealed class TaskCompletionBroadcastServer : UdpServer
    {
        private readonly FileInputChannel _channel;
        private readonly byte[] _jobId;

        public TaskCompletionBroadcastServer(FileInputChannel channel, IPAddress[] localAddresses, int port)
            : base(localAddresses, port, true)
        {
            _channel = channel;
            _jobId = _channel._jobID.ToByteArray();
        }

        protected override void HandleMessage(byte[] message, IPEndPoint remoteEndPoint)
        {
            if (message == null || message.Length < 17)
            {
                _log.Warn("Received invalid task completion broadcast message.");
            }
            else
            {
                for (var x = 0; x < 16; ++x)
                {
                    if (_jobId[x] != message[x])
                    {
                        return; // Job ID doesn't match, not meant for us.
                    }
                }

                var task = new CompletedTask() { JobId = _channel._jobID };

                try
                {
                    var index = 16;
                    int length = message[index++];
                    var taskId = new TaskId(Encoding.UTF8.GetString(message, index, length));
                    index += length;
                    task.TaskAttemptId = new TaskAttemptId(taskId, message[index++]);
                    length = message[index++];
                    var serverName = Encoding.UTF8.GetString(message, index, length);
                    index += length;
                    var port = message[index++] | message[index++] << 8;
                    task.TaskServer = new ServerAddress(serverName, port);
                    task.TaskServerFileServerPort = message[index++] | message[index++] << 8;
                }
                catch (IndexOutOfRangeException)
                {
                    // Should probably do some checks so we can handle this without exceptions.
                    _log.Warn("Received invalid task completion broadcast message.");
                }
                catch (FormatException)
                {
                    _log.Warn("Received invalid task completion broadcast message.");
                }
                catch (ArgumentException)
                {
                    _log.Warn("Received invalid task completion broadcast message.");
                }

                _channel.NotifyTaskCompletion(task);
            }
        }
    }

    private sealed class ServerConnection : IDisposable
    {
        private readonly InputServer _server;
        private TcpClient? _client;
        private WriteBufferedStream? _bufferStream;
        private BinaryWriter? _writer;

        public BinaryReader? Reader { get; private set; }

        public NetworkStream? Stream { get; private set; }

        public ServerConnection(InputServer server)
        {
            _server = server;
        }

#pragma warning disable CS3016 // Arrays as attribute arguments is not CLS-compliant
        [MemberNotNullWhen(true, nameof(Reader), nameof(Stream))]
#pragma warning restore CS3016 // Arrays as attribute arguments is not CLS-compliant
        public bool Connect(Guid jobId, ReadOnlyCollection<int> partitions, int tasksToSkip)
        {
            _client = new TcpClient(_server.TaskServer.HostName, _server.FileServerPort);
            Stream = _client.GetStream();
            Stream.ReadTimeout = 30000;
            Stream.WriteTimeout = 30000;
            _bufferStream = new WriteBufferedStream(Stream);
            _writer = new BinaryWriter(_bufferStream);
            Reader = new BinaryReader(Stream);

            var connectionAccepted = Reader.ReadInt32();
            if (connectionAccepted == 0)
            {
                _log.WarnFormat("Server {0}:{1} is busy.", _server.TaskServer.HostName, _server.FileServerPort);
                return false;
            }

            _writer.Write(jobId.ToByteArray());
            _writer.Write(partitions.Count);
            foreach (var partition in partitions)
            {
                _writer.Write(partition);
            }

            _writer.Write(_server.TasksToDownloadCount - tasksToSkip);
            foreach (var task in _server.TasksToDownload.Skip(tasksToSkip))
            {
                _writer.Write(task.TaskAttemptId!.ToString());
            }

            _writer.Flush();

            return true;
        }

        public void Dispose()
        {
            if (Reader != null)
            {
                Reader.Dispose();
                Reader = null;
            }
            if (_writer != null)
            {
                _writer.Dispose();
                _writer = null;
            }
            if (_bufferStream != null)
            {
                _bufferStream.Dispose();
                _bufferStream = null;
            }
            if (Stream != null)
            {
                Stream.Dispose();
                Stream = null;
            }
            if (_client != null)
            {
                ((IDisposable)_client).Dispose();
                _client = null;
            }
        }
    }


    #endregion

    private const int _pollingInterval = 5000;
    private const int _downloadRetryInterval = 500;
    private const int _downloadRetryIntervalRandomization = 2000;
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileInputChannel));

    private readonly object _progressLock = new object();
    private readonly int _writeBufferSize;
    private readonly string _jobDirectory;
    private readonly Guid _jobID;
    private Thread? _inputPollThread;
    private Thread? _downloadThread;
    private readonly IJobServerClientProtocol _jobServer;
    private readonly string _inputDirectory;
    private bool _isReady;
    private readonly ManualResetEvent _readyEvent = new ManualResetEvent(false);
    private bool _disposed;
    private readonly FileChannelMemoryStorageManager? _memoryStorage;
    private readonly Dictionary<ServerAddress, InputServer> _servers = new Dictionary<ServerAddress, InputServer>();
    private readonly List<InputServer> _orderedServers = new List<InputServer>();
    private HashSet<string>? _tasksLeft; // Only access while _orderedServers is locked.
    private readonly ManualResetEvent _allInputTasksCompleted = new ManualResetEvent(false);
    private readonly Type _inputReaderType;
    private readonly FileChannelOutputType _channelInputType;
    private int _filesRetrieved;
    private int _partitionsCompleted;
    private int _totalPartitions;
    private IMultiInputRecordReader? _reader;
    private volatile bool _hasNonMemoryInputs;
    private TaskCompletionBroadcastServer? _taskCompletionBroadcastServer;
    private readonly Random _rnd = new Random(); // Use only inside _orderedServers lock
    private readonly int _memoryStorageWaitTimeout;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileInputChannel"/> class.
    /// </summary>
    /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
    /// <param name="inputStage">The input stage that this file channel reads from.</param>
    public FileInputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
        : base(taskExecution, inputStage)
    {
        ArgumentNullException.ThrowIfNull(taskExecution);
        ArgumentNullException.ThrowIfNull(inputStage);
        _jobDirectory = taskExecution.Context.LocalJobDirectory;
        _jobID = taskExecution.Context.JobId;
        _jobServer = taskExecution.JetClient.JobServer;
        _inputDirectory = Path.Combine(_jobDirectory, taskExecution.Context.TaskAttemptId.ToString());
        if (!Directory.Exists(_inputDirectory))
        {
            Directory.CreateDirectory(_inputDirectory);
        }
        // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
        // in the case of a join it may not.
        _inputReaderType = typeof(BinaryRecordReader<>).MakeGenericType(InputRecordType);
        _writeBufferSize = (int)taskExecution.JetClient.Configuration.FileChannel.WriteBufferSize;

        _channelInputType = SettingsDictionary.GetJobOrStageSetting(taskExecution.Context.JobConfiguration, inputStage, JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.Spill);

        var memoryStorageSize = (long)TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.MemoryStorageSize, TaskExecution.JetClient.Configuration.FileChannel.MemoryStorageSize);
        if (memoryStorageSize > 0)
        {
            _memoryStorage = FileChannelMemoryStorageManager.GetInstance(memoryStorageSize);
            _memoryStorage.StreamRemoved += new EventHandler(_memoryStorage_StreamRemoved);
            _memoryStorage.WaitingForBuffer += _memoryStorage_WaitingForBuffer;
            _memoryStorageWaitTimeout = TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.MemoryStorageWaitTimeout, TaskExecution.JetClient.Configuration.FileChannel.MemoryStorageWaitTimeout);
        }
    }

    /// <summary>
    /// Gets the number of bytes read from the local disk.
    /// </summary>
    /// <remarks>
    /// This property returns the total amount of data read from the local disk. This includes the compressed size of any local input files, and
    /// any downloaded input files that could not be stored in memory.
    /// </remarks>
    public long LocalBytesRead { get; private set; }

    /// <summary>
    /// Gets or sets the number of bytes written to the local disk.
    /// </summary>
    /// <value>The local bytes written.</value>
    /// <remarks>
    /// This property returns the total amount of data written to the local disk. This equals the combined size of any downloaded input files
    /// that could not be stored in memory.
    /// </remarks>
    public long LocalBytesWritten { get; private set; }

    /// <summary>
    /// Gets the number of bytes read from the network. This is always the compressed figure.
    /// </summary>
    public long NetworkBytesRead { get; private set; }

    /// <summary>
    /// Gets the number of bytes written over the network.
    /// </summary>
    /// <value>The network bytes written.</value>
    public long NetworkBytesWritten
    {
        get { return 0L; }
    }

    /// <summary>
    /// Gets the additional progress value.
    /// </summary>
    /// <value>The additional progress value.</value>
    /// <remarks>
    /// This property is thread safe.
    /// </remarks>
    public float AdditionalProgress
    {
        get
        {
            lock (_progressLock)
            {
                if (_totalPartitions == 0 || InputTaskIds.Count == 0)
                {
                    return 0f;
                }

                return (_partitionsCompleted + ((_filesRetrieved * ActivePartitions.Count) / (float)InputTaskIds.Count)) / _totalPartitions;
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether the input channel uses memory storage to store inputs.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
    /// </value>
    public override bool UsesMemoryStorage
    {
        get { return _memoryStorage != null; }
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
        get
        {
            if (_memoryStorage == null)
            {
                return 0.0f;
            }
            else if (_hasNonMemoryInputs)
            {
                return 1.0f;
            }
            else
            {
                return _memoryStorage.Level;
            }
        }
    }

    /// <summary>
    /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
    /// </summary>
    /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
    /// <remarks>
    /// This function will create a <see cref="MultiRecordReader{T}"/> that serializes the data from all the different input tasks.
    /// </remarks>
    public override IRecordReader CreateRecordReader()
    {
        if (_inputPollThread != null)
        {
            throw new InvalidOperationException("A record reader for this channel was already created.");
        }

        _reader = CreateChannelRecordReader();

        _totalPartitions = ActivePartitions.Count;
        StartThreads();

        // Wait until the reader has at least one input.
        _readyEvent.WaitOne();
        return _reader;
    }

    /// <summary>
    /// Assigns additional partitions to this input channel.
    /// </summary>
    /// <param name="additionalPartitions">The additional partitions.</param>
    /// <remarks>
    /// <para>
    ///   This method will only be called after the task finished processing all previously assigned partitions.
    /// </para>
    /// <para>
    ///   This method will never be called if <see cref="ChannelConfiguration.PartitionsPerTask"/> is 1
    ///   or <see cref="ChannelConfiguration.DisableDynamicPartitionAssignment"/> is <see langword="true"/>.
    /// </para>
    /// </remarks>
    public override void AssignAdditionalPartitions(IList<int> additionalPartitions)
    {
        if (!_allInputTasksCompleted.WaitOne(0))
        {
            throw new InvalidOperationException("Cannot assign additional partitions until the current partitions have finished downloading.");
        }

        // Just making sure the threads have exited.
        _downloadThread?.Join();
        _inputPollThread?.Join();

        lock (_progressLock)
        {
            _filesRetrieved = 0;
            _partitionsCompleted += ActivePartitions.Count;

            base.AssignAdditionalPartitions(additionalPartitions);

            _totalPartitions += ActivePartitions.Count;
            _allInputTasksCompleted.Reset();

            StartThreads();
        }
    }

    #region IDisposable Members

    /// <summary>
    /// Cleans up all the resources held by this class.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    #endregion

    /// <summary>
    /// Cleans up all the resources held by this class.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to clean up managed and unmanaged resources; <see langword="false" /> to clean up unmanaged resources only.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed && disposing)
        {
            _readyEvent.Dispose();
            _allInputTasksCompleted.Dispose();
            if (_taskCompletionBroadcastServer != null)
            {
                _taskCompletionBroadcastServer.Dispose();
            }

            _memoryStorage?.Dispose();
            _reader?.Dispose();
        }
        lock (_orderedServers)
        {
            _disposed = true;
            // Wake up the download thread so it can exit.
            Monitor.Pulse(_orderedServers);
        }
    }

    private void InputPollThread()
    {
        try
        {
            bool hasTasksLeft;
            lock (_tasksLeft!)
            {
                hasTasksLeft = _tasksLeft.Count > 0;
                _log.InfoFormat("Start checking for output file completion of {0} tasks, {1} partitions, timeout {2}ms", _tasksLeft.Count, ActivePartitions.Count, _pollingInterval);
            }

            string[]? tasksLeftArray = null;

            while (!_disposed && hasTasksLeft)
            {
                TaskExecution.ReportProgress(); // Ping the job server for progress to ensure our task doesn't time out while waiting for input.

                lock (_orderedServers)
                {
                    if (tasksLeftArray == null || _tasksLeft.Count != tasksLeftArray.Length)
                    {
                        tasksLeftArray = _tasksLeft.ToArray();
                    }
                }

                // Tasks left count may have reached 0 in between the event timing out and lock acquisition above.
                if (tasksLeftArray.Length > 0)
                {
                    var completedTasks = _jobServer.CheckTaskCompletion(_jobID, tasksLeftArray);
                    if (completedTasks != null && completedTasks.Length > 0)
                    {
                        _log.InfoFormat("Received {0} new completed tasks.", completedTasks.Length);

                        lock (_orderedServers)
                        {
                            foreach (var task in completedTasks)
                            {
                                AddCompletedTaskForDownloading(task);
                            }
                            // Wake up the download thread.
                            Monitor.Pulse(_orderedServers);

                            hasTasksLeft = _tasksLeft.Count > 0;
                        }
                    }
                }
                else
                {
                    hasTasksLeft = false;
                }

                if (hasTasksLeft)
                {
                    hasTasksLeft = !_allInputTasksCompleted.WaitOne(_pollingInterval);
                }
            }

            lock (_orderedServers)
            {
                if (_taskCompletionBroadcastServer != null)
                {
                    _taskCompletionBroadcastServer.Dispose();
                }

                _allInputTasksCompleted.Set();
                // Just making sure the download thread wakes up after this flag is set.
                Monitor.Pulse(_orderedServers);
            }
            if (_disposed)
            {
                _log.Info("Input poll thread aborted because the object was disposed.");
            }
            else
            {
                _log.Info("All files are available.");
            }
        }
        catch (ObjectDisposedException ex)
        {
            // This happens if the thread using the input reader doesn't process all records and disposes the object before
            // we're done here. We ignore it.
            Debug.Assert(ex.ObjectName == "MultiRecordReader");
            _log.WarnFormat("MultiRecordReader was disposed prematurely; object name = \"{0}\"", ex.ObjectName);
        }
    }

    private void AddCompletedTaskForDownloading(CompletedTask task)
    {
        if (_tasksLeft!.Remove(task.TaskAttemptId!.TaskId.ToString()))
        {
            if (!_servers.TryGetValue(task.TaskServer!, out var server))
            {
                server = new InputServer(task.TaskServer!, task.TaskServerFileServerPort);
                _servers.Add(task.TaskServer!, server);
                // Randomize the position of the server in the list to prevent all tasks hitting the same server.
                // We don't do this using insert (because that'd be slower) and we leave open the possility of the new item staying at the end of the list.
                _orderedServers.Add(server);
                _orderedServers.Swap(_orderedServers.Count - 1, _rnd.Next(_orderedServers.Count));
            }
            server.AddCompletedTask(task);
        }
    }

    private void NotifyTaskCompletion(CompletedTask task)
    {
        lock (_orderedServers)
        {
            _log.DebugFormat("Received notification of task {0} completion through job server broadcast.", task.TaskAttemptId);
            AddCompletedTaskForDownloading(task);

            Monitor.Pulse(_orderedServers);

            if (_tasksLeft!.Count == 0)
            {
                _allInputTasksCompleted.Set();
            }
        }
    }

    private void DownloadThread()
    {
        var reader = _reader!;
        var servers = new List<InputServer>();
        while (WaitForTasksToDownload(servers))
        {
            Debug.Assert(servers.Count > 0);

            var noSucceededDownloads = true;
            foreach (var server in servers)
            {
                if (RetrieveInputFiles(reader, server))
                {
                    noSucceededDownloads = false; // We got at least one that succeeded.
                }
            }

            if (noSucceededDownloads)
            {
                int interval;
                lock (_orderedServers)
                {
                    interval = _downloadRetryInterval + _rnd.Next(_downloadRetryIntervalRandomization);
                    // Re-randomize the list of servers if we couldn't download anything.
                    _orderedServers.Randomize(_rnd);
                }
                _log.InfoFormat("Couldn't download any files, will retry after {0}ms.", interval);
                Thread.Sleep(interval); // If we couldn't download any of the files, we will wait a bit to prevent hammering the servers
            }
        }

        TaskExecution.ChannelStatusMessage = null; // Clear the status message when we're finished.

        if (_disposed)
        {
            _log.Info("Download thread aborted because the object was disposed.");
        }
        else
        {
            _log.Info("All files are downloaded.");
        }
    }

    private bool WaitForTasksToDownload(List<InputServer> servers)
    {
        servers.Clear();
        lock (_orderedServers)
        {
            do
            {
                foreach (var server in _orderedServers)
                {
                    if (server.UpdateTasksToDownload())
                    {
                        servers.Add(server);
                    }
                }

                if (servers.Count == 0)
                {
                    if (_allInputTasksCompleted.WaitOne(0))
                    {
                        return false;
                    }

                    Monitor.Wait(_orderedServers);
                }
            } while (!_disposed && servers.Count == 0);
        }

        return !_disposed;
    }

    private bool RetrieveInputFiles(IMultiInputRecordReader reader, InputServer server)
    {
        int downloadedTaskCount;

        if (!InputStage.OutputChannel!.ForceFileDownload && server.IsLocal)
        {
            downloadedTaskCount = UseLocalFilesForInput(server, reader);
        }
        else
        {
            downloadedTaskCount = DownloadFiles(server, reader);
            if (downloadedTaskCount == 0)
            {
                return false; // Couldn't download because the server was busy
            }
        }

        server.NotifyDownloadSuccess(downloadedTaskCount);
        var files = Interlocked.Add(ref _filesRetrieved, downloadedTaskCount);
        TaskExecution.ChannelStatusMessage = string.Format(CultureInfo.InvariantCulture, "Downloaded {0} of {1} input files.", files, InputTaskIds.Count);

        if (!_isReady)
        {
            // When we're using a MultiRecordReader we should become ready after the first downloaded file.
            _log.Info("Input channel is now ready.");
            _isReady = true;
            _readyEvent.Set();
        }

        return true;
    }

    private int UseLocalFilesForInput(InputServer server, IMultiInputRecordReader reader)
    {
        var taskServer = JetClient.CreateTaskServerClient(server.TaskServer);
        foreach (var task in server.TasksToDownload)
        {
            IList<RecordInput> inputs = new List<RecordInput>(ActivePartitions.Count);
            var taskOutputDirectory = taskServer.GetOutputFileDirectory(task.JobId);

            _log.InfoFormat("Using local input files from task {0} for {1} partitions.", task.TaskAttemptId, ActivePartitions.Count);
            UseLocalPartitionFileForInput(task, inputs, taskOutputDirectory);
            reader.AddInput(inputs);
        }
        return server.TasksToDownloadCount;
    }

    private void UseLocalPartitionFileForInput(CompletedTask task, IList<RecordInput> inputs, string taskOutputDirectory)
    {

        var outputFileName = FileOutputChannel.CreateChannelFileName(task.TaskAttemptId!.ToString());
        var fileName = Path.Combine(taskOutputDirectory, outputFileName);
        using (var index = new PartitionFileIndex(fileName))
        {
            foreach (var partition in ActivePartitions)
            {
                var indexEntries = index.GetEntriesForPartition(partition);
                if (indexEntries == null)
                {
                    _log.DebugFormat("Local input file {0} partition {1} is empty.", fileName, partition);
                    inputs.Add(new EmptyRecordInput(InputRecordType, task.TaskAttemptId.TaskId.ToString()));
                }
                else
                {
                    LocalBytesRead += indexEntries.Sum(e => e.CompressedSize);
                    inputs.Add(new PartitionFileRecordInput(_inputReaderType, fileName, indexEntries, task.TaskAttemptId.TaskId.ToString(), _channelInputType == FileChannelOutputType.SortSpill, _reader!.AllowRecordReuse, _reader.BufferSize, CompressionType));
                }
            }
        }
    }

    private int DownloadFiles(InputServer server, IMultiInputRecordReader targetReader)
    {
        var port = server.FileServerPort;

        _log.InfoFormat(CultureInfo.InvariantCulture, "Downloading tasks {0} input files from server {1}:{2}.", server.TasksToDownload.ToDelimitedString(), server.TaskServer.HostName, server.FileServerPort);

        var downloadedTaskCount = 0;
        try
        {
            using (var connection = new ServerConnection(server))
            {
                if (!connection.Connect(_jobID, ActivePartitions, 0))
                {
                    return downloadedTaskCount;
                }

                foreach (var task in server.TasksToDownload)
                {
                    var size = connection.Reader.ReadInt64();
                    using var reservation = _memoryStorage?.WaitForSpaceAndReserve(size, connection, _memoryStorageWaitTimeout);
                    if (reservation != null && reservation.Waited)
                    {
                        if (!connection.Connect(_jobID, ActivePartitions, downloadedTaskCount))
                        {
                            return downloadedTaskCount;
                        }

                        if (size != connection.Reader.ReadInt64())
                        {
                            throw new InvalidOperationException("Task output size changed after reconnect.");
                        }
                    }

                    var downloadedFiles = new List<RecordInput>(ActivePartitions.Count);
                    foreach (var partition in ActivePartitions)
                    {
                        DownloadPartition(task, downloadedFiles, connection, partition, reservation);
                    }
                    targetReader.AddInput(downloadedFiles);
                    ++downloadedTaskCount;
                }
                _log.Debug("Download complete.");

                return downloadedTaskCount;
            }
        }
        catch (SocketException ex)
        {
            // TODO: If this happens too often, we need to recover somehow.
            _log.Error(string.Format(CultureInfo.InvariantCulture, "Error contacting server {0}:{1}.", server.TaskServer.HostName, port), ex);
            return downloadedTaskCount;
        }
        catch (IOException ex)
        {
            if (ex.InnerException is SocketException)
            {
                _log.Error(string.Format(CultureInfo.InvariantCulture, "Error contacting server {0}:{1}.", server.TaskServer.HostName, port), ex);
                return downloadedTaskCount;
            }
            else
            {
                throw;
            }
        }
    }

    private void DownloadPartition(CompletedTask task, List<RecordInput> downloadedFiles, ServerConnection connection, int partition, FileChannelMemoryStorageManager.Reservation? reservation)
    {
        Debug.Assert(connection.Stream != null && connection.Reader != null);
        var size = connection.Reader.ReadInt64();
        if (size > 0)
        {
            var segmentCount = 0;
            var uncompressedSize = connection.Reader.ReadInt64();
            segmentCount = connection.Reader.ReadInt32();
            string? targetFile = null;

            if (reservation == null)
            {
                _hasNonMemoryInputs = true;
                targetFile = Path.Combine(_inputDirectory, string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}_part{1}.input", task.TaskAttemptId!.TaskId, partition));
                using (var fileStream = File.Create(targetFile, _writeBufferSize))
                {
                    connection.Stream.CopySize(fileStream, size, _writeBufferSize);
                }
                downloadedFiles.Add(new FileRecordInput(_inputReaderType, targetFile, task.TaskAttemptId.TaskId.ToString(), uncompressedSize, TaskExecution.JetClient.Configuration.FileChannel.DeleteIntermediateFiles, _channelInputType == FileChannelOutputType.SortSpill, segmentCount, _reader!.AllowRecordReuse, _reader.BufferSize, _reader.CompressionType));
                _log.DebugFormat("Input stored in local file {0}.", targetFile);
                // We are writing this file to disk and reading it back again, so we need to update this.
                LocalBytesRead += size;
                LocalBytesWritten += size;
            }
            else
            {
                Stream memoryStream = reservation.CreateStream(size);
                connection.Stream.CopySize(memoryStream, size);
                memoryStream.Position = 0;
                Stream checksumStream;
                checksumStream = new SegmentedChecksumInputStream(memoryStream, segmentCount, CompressionType, uncompressedSize);
                downloadedFiles.Add(new StreamRecordInput(_inputReaderType, checksumStream, true, task.TaskAttemptId!.TaskId.ToString(), _channelInputType == FileChannelOutputType.SortSpill, TaskExecution.Context.StageConfiguration.AllowRecordReuse));
            }
            NetworkBytesRead += size;
        }
        else if (size == 0)
        {
            _log.DebugFormat("Input partition {0} is empty.", partition);
            downloadedFiles.Add(new EmptyRecordInput(InputRecordType, task.TaskAttemptId!.TaskId.ToString()));
        }
        else
        {
            throw new InvalidOperationException(); // TODO: Recover from this.
        }
    }

    private void StartThreads()
    {
        lock (_orderedServers)
        {
            _tasksLeft = new HashSet<string>(InputTaskIds);
            _inputPollThread = new Thread(InputPollThread) { Name = "FileInputChannelPolling", IsBackground = true };
            _inputPollThread.Start();
            _downloadThread = new Thread(DownloadThread) { Name = "FileInputChannelDownload", IsBackground = true };
            _downloadThread.Start();

            var broadcastPort = TaskExecution.JetClient.Configuration.JobServer.BroadcastPort;
            if (broadcastPort > 0)
            {
                // Only supports IPv4 at the moment.
                _taskCompletionBroadcastServer = new TaskCompletionBroadcastServer(this, new[] { IPAddress.Any }, broadcastPort);
                _taskCompletionBroadcastServer.Start();
            }
        }
    }

    private void _memoryStorage_StreamRemoved(object? sender, EventArgs e)
    {
        _hasNonMemoryInputs = false;
    }

    private void _memoryStorage_WaitingForBuffer(object? sender, MemoryStorageFullEventArgs e)
    {
        OnMemoryStorageFull(e);
    }

}
