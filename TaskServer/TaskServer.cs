// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Rpc;

namespace TaskServerApplication
{
    public sealed class TaskServer : ITaskServerUmbilicalProtocol, ITaskServerClientProtocol, IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskServer));

        private readonly int _heartbeatInterval = 3000;
        private readonly AutoResetEvent _heartbeatEvent = new AutoResetEvent(false);
        private readonly ManualResetEvent _shutdownEvent = new ManualResetEvent(false);
        private readonly IJobServerHeartbeatProtocol _jobServer;
        private readonly List<JetHeartbeatData> _pendingHeartbeatData = new List<JetHeartbeatData>();
        private readonly TaskRunner _taskRunner;
        private static readonly object _startupLock = new object();
        private readonly FileChannelServer _fileServer;
        private readonly Dictionary<Guid, JobInfo> _jobs = new Dictionary<Guid, JobInfo>();
        private readonly bool _immediateCompletedTaskNotification;

        private TaskServer()
            : this(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration())
        {
        }

        private TaskServer(JetConfiguration config, DfsConfiguration dfsConfiguration)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            Configuration = config;
            DfsConfiguration = dfsConfiguration;

            if (string.IsNullOrWhiteSpace(config.TaskServer.TaskDirectory))
                throw new InvalidOperationException("TaskServer task directory is not configured.");

            if (!System.IO.Directory.Exists(config.TaskServer.TaskDirectory))
                System.IO.Directory.CreateDirectory(config.TaskServer.TaskDirectory);

            _jobServer = JetClient.CreateJobServerHeartbeatClient(config);
            _heartbeatInterval = config.TaskServer.HeartbeatInterval;
            _immediateCompletedTaskNotification = config.TaskServer.ImmediateCompletedTaskNotification;

            _taskRunner = new TaskRunner(this);

            LocalAddress = new ServerAddress(Dns.GetHostName(), Configuration.TaskServer.Port);

            var addresses = TcpServer.GetDefaultListenerAddresses(Configuration.TaskServer.ListenIPv4AndIPv6);

            _fileServer = new FileChannelServer(this, addresses, Configuration.TaskServer.FileServerPort, Configuration.TaskServer.FileServerMaxConnections, Configuration.TaskServer.FileServerMaxIndexCacheSize);
            _fileServer.Start();
        }

        public static TaskServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }
        public DfsConfiguration DfsConfiguration { get; private set; }
        public ServerAddress LocalAddress { get; private set; }

        public static void Run()
        {
            Run(JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        public static void Run(JetConfiguration jetConfig, DfsConfiguration dfsConfig)
        {
            _log.Info("-----Task server is starting-----");
            _log.LogEnvironmentInformation();
            // Prevent type references in job configurations from loading assemblies into the task server.
            TypeReference.ResolveTypes = false;

            lock (_startupLock)
            {
                Instance = new TaskServer(jetConfig, dfsConfig);

                RpcHelper.RegisterServerChannels(jetConfig.TaskServer.Port, jetConfig.TaskServer.ListenIPv4AndIPv6);
                RpcHelper.RegisterService("TaskServer", Instance);
            }

            Instance.RunInternal();
        }

        public static void Shutdown()
        {
            lock (_startupLock)
            {
                Instance.ShutdownInternal();

                RpcHelper.UnregisterServerChannels(Instance.Configuration.TaskServer.Port);

                Instance.Dispose();
                Instance = null;
            }
        }

        public void NotifyTaskStatusChanged(Guid jobID, TaskAttemptId taskAttemptId, TaskAttemptStatus newStatus, TaskProgress progress, TaskMetrics metrics)
        {
            AddDataForNextHeartbeat(new TaskStatusChangedJetHeartbeatData(jobID, taskAttemptId, newStatus, progress, metrics));
            if (_immediateCompletedTaskNotification && newStatus == TaskAttemptStatus.Completed)
                _heartbeatEvent.Set();
        }

        public string GetJobDirectory(Guid jobID)
        {
            return System.IO.Path.Combine(Configuration.TaskServer.TaskDirectory, "job_" + jobID.ToString());
        }

        #region ITaskServerUmbilicalProtocol Members

        public void ReportCompletion(Guid jobID, TaskAttemptId taskAttemptId, TaskMetrics metrics)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));
            var fullTaskID = Job.CreateFullTaskId(jobID, taskAttemptId);
            _log.DebugFormat("ReportCompletion, fullTaskID = \"{0}\"", fullTaskID);
            _taskRunner.ReportCompletion(fullTaskID, metrics);
        }

        public void ReportProgress(Guid jobId, TaskAttemptId taskAttemptId, TaskProgress progress)
        {
            _taskRunner.ReportProgress(Job.CreateFullTaskId(jobId, taskAttemptId), progress);
        }

        public void ReportError(Guid jobId, TaskAttemptId taskAttemptId, string failureReason)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));
            _taskRunner.ReportError(Job.CreateFullTaskId(jobId, taskAttemptId), failureReason);
        }

        public void RegisterTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId, int port)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));
            if (port <= 0)
                throw new ArgumentOutOfRangeException(nameof(port), "Port must be greater than zero.");

            var fullTaskId = Job.CreateFullTaskId(jobId, taskAttemptId);
            _log.InfoFormat("Task {0} has is registering TCP channel port {1}.", fullTaskId, port);
            _taskRunner.RegisterTcpChannelPort(fullTaskId, port);
        }

        public string DownloadDfsFile(Guid jobId, string dfsPath)
        {
            var job = GetJobInfo(jobId, true);

            lock (job)
            {
                return job.DownloadDfsFile(dfsPath);
            }
        }

        #endregion

        #region ITaskServerClientProtocol Members

        public int FileServerPort
        {
            get { return Configuration.TaskServer.FileServerPort; }
        }

        public TaskAttemptStatus GetTaskStatus(Guid jobId, TaskAttemptId taskAttemptId)
        {
            var fullTaskID = Job.CreateFullTaskId(jobId, taskAttemptId);
            _log.DebugFormat("GetTaskStatus, fullTaskID = \"{0}\"", fullTaskID);
            var status = _taskRunner.GetTaskStatus(fullTaskID);
            _log.DebugFormat("Task {0} status is {1}.", fullTaskID, status);
            return status;
        }

        public string GetOutputFileDirectory(Guid jobId)
        {
            _log.DebugFormat("GetOutputFileDirectory, jobId = \"{0}\"", jobId);
            return GetJobDirectory(jobId);
        }

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return LogFileHelper.GetLogFileContents("TaskServer", kind, maxSize);
        }

        public string GetTaskLogFileContents(Guid jobId, TaskAttemptId taskAttemptId, int maxSize)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));

            _log.DebugFormat("GetTaskLogFileContents; jobId = {{{0}}}, taskAttemptId = \"{1}\", maxSize = {2}", jobId, taskAttemptId, maxSize);
            var jobDirectory = GetJobDirectory(jobId);
            var logFileName = System.IO.Path.Combine(jobDirectory, taskAttemptId.ToString() + ".log");
            if (System.IO.File.Exists(logFileName))
            {
                using (var stream = System.IO.File.Open(logFileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
                using (var reader = new System.IO.StreamReader(stream))
                {
                    if (stream.Length > maxSize)
                    {
                        stream.Position = stream.Length - maxSize;
                        reader.ReadLine(); // Scan to the first new line.
                    }
                    return reader.ReadToEnd();
                }
            }
            return null;
        }

        public byte[] GetCompressedTaskLogFiles(Guid jobId)
        {
            _log.DebugFormat("GetCompressedTaskLogFiles; jobId = {{{0}}}", jobId);
            var jobDirectory = GetJobDirectory(jobId);
            if (System.IO.Directory.Exists(jobDirectory))
            {
                var logFiles = System.IO.Directory.GetFiles(jobDirectory, "*.log");
                if (logFiles.Length > 0)
                {
                    using (var outputStream = new MemoryStream())
                    using (var zipStream = new ICSharpCode.SharpZipLib.Zip.ZipOutputStream(outputStream))
                    {
                        zipStream.SetLevel(9);

                        foreach (var logFile in logFiles)
                        {
                            zipStream.PutNextEntry(new ICSharpCode.SharpZipLib.Zip.ZipEntry(System.IO.Path.GetFileName(logFile)));
                            using (var stream = System.IO.File.Open(logFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
                            {
                                stream.CopyTo(zipStream);
                            }
                        }

                        zipStream.Finish();

                        return outputStream.ToArray();
                    }
                }
            }

            return null;
        }

        public string GetTaskProfileOutput(Guid jobId, TaskAttemptId taskAttemptId)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));

            _log.DebugFormat("GetTaskProfileOutput; jobId = {{{0}}}, taskAttemptId = \"{1}\"", jobId, taskAttemptId);
            var jobDirectory = GetJobDirectory(jobId);
            var profileOutputFileName = System.IO.Path.Combine(jobDirectory, taskAttemptId.ToString() + "_profile.txt");
            if (System.IO.File.Exists(profileOutputFileName))
            {
                using (var stream = System.IO.File.Open(profileOutputFileName, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
                using (var reader = new System.IO.StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
            return null;
        }

        public int GetTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));
            _log.DebugFormat("GetTcpChannelPort; jobId = {{{0}}}, taskId = \"{1}\"", jobId, taskAttemptId);
            return _taskRunner.GetTcpChannelPort(Job.CreateFullTaskId(jobId, taskAttemptId));
        }

        #endregion

        private void RunInternal()
        {
            AddDataForNextHeartbeat(new InitialStatusJetHeartbeatData() { TaskSlots = Configuration.TaskServer.TaskSlots, FileServerPort = Configuration.TaskServer.FileServerPort });
            var handles = new WaitHandle[] { _heartbeatEvent, _shutdownEvent };

            do
            {
                SendHeartbeat();
                _taskRunner.KillTimedOutTasks();
            } while (WaitHandle.WaitAny(handles, _heartbeatInterval) != 1);
        }

        private void ShutdownInternal()
        {
            _taskRunner.Stop();
            if (_fileServer != null)
                _fileServer.Stop();
            _shutdownEvent.Set();
            RpcHelper.AbortRetries();
            RpcHelper.CloseConnections();
            _log.Info("-----Task server is shutting down-----");
        }

        private void SendHeartbeat()
        {
            JetHeartbeatData[] data = null;
            lock (_pendingHeartbeatData)
            {
                if (_pendingHeartbeatData.Count > 0)
                {
                    data = _pendingHeartbeatData.ToArray();
                    _pendingHeartbeatData.Clear();
                }
            }

            JetHeartbeatResponse[] responses = null;

            RpcHelper.TryRemotingCall(() => responses = _jobServer.Heartbeat(LocalAddress, data), _heartbeatInterval, -1);

            if (responses != null)
                ProcessResponses(responses);
        }

        private void ProcessResponses(JetHeartbeatResponse[] responses)
        {
            foreach (var response in responses)
            {
                if (response.Command != TaskServerHeartbeatCommand.None)
                    _log.InfoFormat("Received {0} command.", response.Command);

                switch (response.Command)
                {
                case TaskServerHeartbeatCommand.ReportStatus:
                    AddDataForNextHeartbeat(new InitialStatusJetHeartbeatData() { TaskSlots = Configuration.TaskServer.TaskSlots, FileServerPort = Configuration.TaskServer.FileServerPort });
                    break;
                case TaskServerHeartbeatCommand.RunTask:
                    var runResponse = (RunTaskJetHeartbeatResponse)response;
                    _log.InfoFormat("Received run task command for task {{{0}}}_{1}.", runResponse.Job.JobId, runResponse.TaskAttemptId);
                    _taskRunner.AddTask(runResponse);
                    break;
                case TaskServerHeartbeatCommand.KillTask:
                    var killResponse = (KillTaskJetHeartbeatResponse)response;
                    _log.InfoFormat("Received kill task command for task {{{0}}}_{1}.", killResponse.JobId, killResponse.TaskAttemptId);
                    _taskRunner.KillTask(killResponse);
                    break;
                case TaskServerHeartbeatCommand.CleanupJob:
                    var cleanupResponse = (CleanupJobJetHeartbeatResponse)response;
                    _log.InfoFormat("Received cleanup job command for job {{{0}}}.", cleanupResponse.JobId);
                    _taskRunner.CleanupJobTasks(cleanupResponse.JobId);
                    // Do file clean up asynchronously since it could take a long time.
                    ThreadPool.QueueUserWorkItem((state) => CleanupJobFiles((Guid)state), cleanupResponse.JobId);
                    break;
                }
            }
        }

        private void AddDataForNextHeartbeat(JetHeartbeatData data)
        {
            lock (_pendingHeartbeatData)
                _pendingHeartbeatData.Add(data);
        }

        private void CleanupJobFiles(Guid jobId, string directory)
        {
            foreach (var file in System.IO.Directory.GetFiles(directory))
            {
                if (file.EndsWith(".output", StringComparison.Ordinal) || file.EndsWith(".input", StringComparison.Ordinal))
                {
                    _log.DebugFormat("Job {0} cleanup: deleting file {1}.", jobId, file);
                    System.IO.File.Delete(file);
                }
            }
            foreach (var subDirectory in Directory.GetDirectories(directory))
                CleanupJobFiles(jobId, subDirectory);
        }

        private void CleanupJobFiles(Guid jobId)
        {
            lock (_jobs)
            {
                _jobs.Remove(jobId);
            }

            try
            {
                if (Configuration.FileChannel.DeleteIntermediateFiles)
                {
                    var jobDirectory = GetJobDirectory(jobId);
                    CleanupJobFiles(jobId, jobDirectory);

                    var downloadDirectory = Path.Combine(jobDirectory, "dfs");
                    if (Directory.Exists(downloadDirectory))
                        Directory.Delete(downloadDirectory, true);
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                _log.Error("Failed to clean up job files", ex);
            }
            catch (IOException ex)
            {
                _log.Error("Failed to clean up job files", ex);
            }
        }

        private JobInfo GetJobInfo(Guid jobId, bool create)
        {
            JobInfo job;
            lock (_jobs)
            {
                if (!_jobs.TryGetValue(jobId, out job))
                {
                    if (create)
                    {
                        job = new JobInfo(jobId);
                        _jobs.Add(jobId, job);
                    }
                    else
                        return null;
                }
            }
            return job;
        }

        public void Dispose()
        {
            _heartbeatEvent?.Dispose();
            _shutdownEvent?.Dispose();
        }
    }
}
