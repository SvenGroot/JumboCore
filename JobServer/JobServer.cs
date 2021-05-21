// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Xml.Linq;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Scheduling;
using Ookii.Jumbo.Rpc;
using Ookii.Jumbo.Topology;

namespace JobServerApplication
{
    public sealed class JobServer : IJobServerHeartbeatProtocol, IJobServerClientProtocol, IJobServerTaskProtocol, IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobServer));

        private readonly ConcurrentDictionary<ServerAddress, TaskServerInfo> _taskServers = new ConcurrentDictionary<ServerAddress, TaskServerInfo>();
        private volatile bool _taskServersModified;
        private readonly NetworkTopology _topology; // Lock _topology when accessing.
        private readonly Dictionary<Guid, Job> _pendingJobs = new Dictionary<Guid, Job>(); // Jobs that have been created but aren't running yet.
        private readonly ConcurrentDictionary<Guid, JobInfo> _jobs = new ConcurrentDictionary<Guid, JobInfo>();
        private readonly List<JobInfo> _orderedJobs = new List<JobInfo>(); // Jobs in the order that they should be scheduled.
        private readonly Dictionary<Guid, JobInfo> _finishedJobs = new Dictionary<Guid, JobInfo>();
        private readonly List<JobInfo> _jobsNeedingCleanup = new List<JobInfo>();
        private readonly FileSystemClient _fileSystemClient;
        private readonly ITaskScheduler _scheduler;
        private readonly object _schedulerLock = new object();
        private readonly ServerAddress _localAddress;
        private readonly BlockingCollection<ManualResetEventSlim> _schedulerRequests = new BlockingCollection<ManualResetEventSlim>();
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private Thread _schedulerThread;
        private CancellationTokenSource _schedulerCancellation;
        private readonly object _schedulerThreadLock = new object();
        private readonly object _archiveLock = new object();
        private const int _schedulerTimeoutMilliseconds = 30000;
        private const string _archiveFileName = "archive";
        private readonly TaskCompletionBroadcaster _broadcaster; // Only access inside scheduler lock

        private JobServer(JumboConfiguration jumboConfiguration, JetConfiguration jetConfiguration, DfsConfiguration dfsConfiguration)
        {
            if( jumboConfiguration == null )
                throw new ArgumentNullException(nameof(jumboConfiguration));
            if( jetConfiguration == null )
                throw new ArgumentNullException(nameof(jetConfiguration));
            if( dfsConfiguration == null )
                throw new ArgumentNullException(nameof(dfsConfiguration));

            Configuration = jetConfiguration;
            _topology = new NetworkTopology(jumboConfiguration);
            _fileSystemClient = FileSystemClient.Create(dfsConfiguration);
            _localAddress = new ServerAddress(ServerContext.LocalHostName, jetConfiguration.JobServer.Port);

            _scheduler = (ITaskScheduler)JetActivator.CreateInstance(Type.GetType(jetConfiguration.JobServer.Scheduler), dfsConfiguration, jetConfiguration, null);

            if( Configuration.JobServer.DataInputSchedulingMode == SchedulingMode.Default )
            {
                _log.Warn("DataInputSchedulingMode was set to SchedulingMode.Default; SchedulingMode.MoreServers will be used instead.");
                Configuration.JobServer.DataInputSchedulingMode = SchedulingMode.MoreServers;
            }
            if( Configuration.JobServer.NonDataInputSchedulingMode == SchedulingMode.Default || Configuration.JobServer.NonDataInputSchedulingMode == SchedulingMode.OptimalLocality )
            {
                _log.WarnFormat("NonDataInputSchedulingMode was set to SchedulingMode.{0}; SchedulingMode.MoreServers will be used instead.", Configuration.JobServer.NonDataInputSchedulingMode);
                Configuration.JobServer.NonDataInputSchedulingMode = SchedulingMode.MoreServers;
            }
            if( Configuration.JobServer.BroadcastPort > 0 )
            {
                _broadcaster = new TaskCompletionBroadcaster(Configuration.JobServer.BroadcastAddress, Configuration.JobServer.BroadcastPort);
            }
        }

        public static JobServer Instance { get; private set; }

        public JetConfiguration Configuration { get; private set; }

        public int RackCount
        {
            get
            {
                lock( _topology )
                {
                    return _topology.Racks.Count;
                }
            }
        }

        public static void Run()
        {
            Run(JumboConfiguration.GetConfiguration(), JetConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Run(JumboConfiguration jumboConfiguration, JetConfiguration jetConfiguration, DfsConfiguration dfsConfiguration)
        {
            if( jetConfiguration == null )
                throw new ArgumentNullException(nameof(jetConfiguration));

            _log.Info("-----Job server is starting-----");
            _log.LogEnvironmentInformation();

            // Prevent type references in job configurations from accidentally loading assemblies into the job server.
            TypeReference.ResolveTypes = false;

            Instance = new JobServer(jumboConfiguration, jetConfiguration, dfsConfiguration);
            RpcHelper.RegisterServerChannels(jetConfiguration.JobServer.Port, jetConfiguration.JobServer.ListenIPv4AndIPv6);
            RpcHelper.RegisterService("JobServer", Instance);

            _log.Info("Rpc server started.");
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            _log.Info("-----Job server is shutting down-----");
            RpcHelper.UnregisterServerChannels(Instance.Configuration.JobServer.Port);
            RpcHelper.AbortRetries();
            RpcHelper.CloseConnections();
            Instance.ShutdownInternal();
            Instance.Dispose();
            Instance = null;
        }

        #region IJobServerClientProtocol Members

        public Job CreateJob()
        {
            _log.Debug("CreateJob");
            Guid jobID = Guid.NewGuid();
            string path = _fileSystemClient.Path.Combine(Configuration.JobServer.JetDfsPath, FormattableString.Invariant($"job_{jobID:B}"));
            _fileSystemClient.CreateDirectory(path);
            _fileSystemClient.CreateDirectory(_fileSystemClient.Path.Combine(path, "temp"));
            Job job = new Job(jobID, path);
            lock( _pendingJobs )
            {
                _pendingJobs.Add(jobID, job);
            }
            _log.InfoFormat("Created new job {0}, data path = {1}", jobID, path);
            return job;
        }

        public void RunJob(Guid jobId)
        {
            _log.DebugFormat("RunJob, jobID = {{{0}}}", jobId);

            Job job;
            lock( _pendingJobs )
            {
                if( !_pendingJobs.TryGetValue(jobId, out job) )
                    throw new ArgumentException("Job does not exist or is already running.");
                _pendingJobs.Remove(jobId);
            }

            string configFile = job.GetJobConfigurationFilePath(_fileSystemClient);

            _log.InfoFormat("Starting job {0}.", jobId);
            JobConfiguration config;
            try
            {
                using( Stream stream = _fileSystemClient.OpenFile(configFile) )
                {
                    config = JobConfiguration.LoadXml(stream);
                }
            }
            catch( Exception ex )
            {
                _log.Error($"Could not load job config file {configFile}.", ex);
                throw;
            }


            JobInfo jobInfo = new JobInfo(job, config, _fileSystemClient);
            if( !_jobs.TryAdd(jobId, jobInfo) )
                throw new ArgumentException("The job is already running.");

            lock( _orderedJobs )
            {
                _orderedJobs.Add(jobInfo);
            }

            _log.InfoFormat("Job {0} has entered the running state. Number of tasks: {1}.", jobId, jobInfo.UnscheduledTaskCount);

            RunScheduler();
        }

        public bool AbortJob(Guid jobId)
        {
            lock( _pendingJobs )
            {
                Job job;
                if( _pendingJobs.TryGetValue(jobId, out job) )
                {
                    _pendingJobs.Remove(jobId);
                    _log.InfoFormat("Removed pending job {0} from the job queue.", jobId);
                    return true;
                }
            }

            lock( _schedulerLock )
            {
                JobInfo job;
                if( !_jobs.TryGetValue(jobId, out job) )
                    _log.InfoFormat("Didn't abort job {0} because it wasn't found in the running job list.", jobId);
                else if( job.State == JobState.Running )
                {
                    _log.InfoFormat("Aborting job {0}.", jobId);
                    job.FailureReason = "Job aborted.";
                    job.SchedulerInfo.State = JobState.Failed;
                    FinishOrFailJob(job);
                    return true;
                }
                else
                    _log.InfoFormat("Didn't abort job {0} because it was not running.", jobId);
            }

            return false;
        }

        public ServerAddress GetTaskServerForTask(Guid jobID, string taskID)
        {
            _log.DebugFormat("GetTaskServerForTask, jobID = {{{0}}}, taskID = \"{1}\"", jobID, taskID);
            if( taskID == null )
            throw new ArgumentNullException(nameof(taskID));
            JobInfo job = _jobs[jobID];
            TaskInfo task = job.GetTask(taskID);
            TaskServerInfo server = task.Server; // For thread-safety, we should do only one read of the property.
            return server == null ? null : server.Address;
        }

        public CompletedTask[] CheckTaskCompletion(Guid jobId, string[] taskIds)
        {
            if( taskIds == null )
                throw new ArgumentNullException(nameof(taskIds));
            if( taskIds.Length == 0 )
                throw new ArgumentException("You must specify at least one task.", nameof(taskIds));

            // This method is safe without locking because none of the state of the job it accesses can be changed after the job is created.
            // The exception is task.State, but since that's a single integer value and we're only reading it that's not an issue either.

            JobInfo job = GetRunningOrFinishedJob(jobId);

            List<CompletedTask> result = new List<CompletedTask>();
            foreach( string taskId in taskIds )
            {
                TaskInfo task = job.GetTask(taskId);
                if( task.State == TaskState.Finished )
                {
                    TaskServerInfo server = task.Server; // For thread-safety, do only one read of the property
                    result.Add(new CompletedTask() { JobId = jobId, TaskAttemptId = task.SuccessfulAttempt, TaskServer = server.Address, TaskServerFileServerPort = server.FileServerPort });
                }
            }

            return result.ToArray();
        }

        public JobStatus GetJobStatus(Guid jobId)
        {
            JobInfo job;
            if( !_jobs.TryGetValue(jobId, out job) )
            {
                lock( _finishedJobs )
                {
                    if( !_finishedJobs.TryGetValue(jobId, out job) )
                        return null;
                }
            }
            return job.ToJobStatus();
        }

        public JobStatus[] GetRunningJobs()
        {
            return (from job in _jobs.Values
                    select job.ToJobStatus()).ToArray();
        }

        public JetMetrics GetMetrics()
        {
            JetMetrics result = new JetMetrics()
            {
                JobServer = _localAddress
            };

            result.RunningJobs.AddRange(from job in _jobs.Values where job.State == JobState.Running select job.Job.JobId);
            lock( _finishedJobs )
            {
                result.FinishedJobs.AddRange(from job in _finishedJobs
                                             where job.Value.State == JobState.Finished
                                             select job.Key);
                result.FailedJobs.AddRange(from job in _finishedJobs
                                           where job.Value.State == JobState.Failed
                                           select job.Key);
            }

            result.TaskServers.AddRange(from server in _taskServers.Values
                                        select new TaskServerMetrics()
                                        {
                                            Address = server.Address,
                                            RackId = server.Rack == null ? null : server.Rack.RackId,
                                            LastContactUtc = server.LastContactUtc,
                                            TaskSlots = server.TaskSlots,
                                        });
            result.Capacity = result.TaskServers.Sum(s => s.TaskSlots);
            result.Scheduler = _scheduler.GetType().Name;
            return result;
        }

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return LogFileHelper.GetLogFileContents("JobServer", kind, maxSize);
        }

        public ArchivedJob[] GetArchivedJobs()
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( archiveDir != null )
            {
                string archiveFilePath = Path.Combine(archiveDir, _archiveFileName);
                if( File.Exists(archiveFilePath) )
                {
                    lock( _archiveLock )
                    {
                        using( FileStream stream = File.OpenRead(archiveFilePath) )
                        using( BinaryRecordReader<ArchivedJob> reader = new BinaryRecordReader<ArchivedJob>(stream) )
                        {
                            return reader.EnumerateRecords().ToArray();
                        }
                    }
                }
            }

            return null;
        }

        public JobStatus GetArchivedJobStatus(Guid jobId)
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( archiveDir != null )
            {
                string summaryPath = Path.Combine(archiveDir, jobId.ToString() + "_summary.xml");
                if( File.Exists(summaryPath) )
                {
                    return JobStatus.FromXml(XDocument.Load(summaryPath).Root);
                }
            }

            return null;
        }

        public string GetJobConfigurationFile(Guid jobId, bool archived)
        {
            if( archived )
            {
                string archiveDir = Configuration.JobServer.ArchiveDirectory;
                if( archiveDir != null )
                {
                    string configPath = Path.Combine(archiveDir, jobId.ToString() + "_config.xml");
                    if( File.Exists(configPath) )
                    {
                        return File.ReadAllText(configPath);
                    }
                }
            }
            else
            {
                JobInfo job;
                lock( _finishedJobs )
                {
                    if( _jobs.TryGetValue(jobId, out job) || _finishedJobs.TryGetValue(jobId, out job) )
                    {
                        using( Stream stream = _fileSystemClient.OpenFile(job.Job.GetJobConfigurationFilePath(_fileSystemClient)) )
                        using( StreamReader reader = new StreamReader(stream) )
                        {
                            return reader.ReadToEnd();
                        }
                    }
                }
            }

            return null;
        }

        #endregion

        #region IJobServerTaskProtocol Members

        public int[] GetPartitionsForTask(Guid jobId, TaskId taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException(nameof(taskId));

            JobInfo job;
            if( !_jobs.TryGetValue(jobId, out job) )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            return task.PartitionInfo == null ? null : task.PartitionInfo.GetAssignedPartitions();
        }

        public bool NotifyStartPartitionProcessing(Guid jobId, TaskId taskId, int partitionNumber)
        {
            if( taskId == null )
                throw new ArgumentNullException(nameof(taskId));

            JobInfo job;
            if( !_jobs.TryGetValue(jobId, out job) )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            if( task.PartitionInfo == null )
                throw new ArgumentException("Task doesn't have partitions.");

            return task.PartitionInfo.NotifyStartPartitionProcessing(partitionNumber);
        }

        public int[] GetAdditionalPartitions(Guid jobId, TaskId taskId)
        {
            if( taskId == null )
                throw new ArgumentNullException(nameof(taskId));

            JobInfo job;
            if( !_jobs.TryGetValue(jobId, out job) )
                throw new ArgumentException("Unknown job ID.");
            TaskInfo task = job.GetTask(taskId.ToString());
            if( task.PartitionInfo == null )
                throw new ArgumentException("Task doesn't have partitions.");

            int additionalPartition = task.PartitionInfo.AssignAdditionalPartition();
            if( additionalPartition == -1 )
                return null;
            else
                return new[] { additionalPartition };
        }
        
        #endregion

        #region IJobServerHeartbeatProtocol Members

        public JetHeartbeatResponse[] Heartbeat(Ookii.Jumbo.ServerAddress address, JetHeartbeatData[] data)
        {
            if( address == null )
                throw new ArgumentNullException(nameof(address));

            TaskServerInfo server = _taskServers.GetOrAdd(address, key => new TaskServerInfo(key));

            if( !server.HasReportedStatus )
            {
                if( data == null || !data.Any(d => d is InitialStatusJetHeartbeatData) )
                {
                    _log.WarnFormat("Task server {0} has not sent any status data before (or it was declared dead) but didn't send any.", address);
                    return new[] { new JetHeartbeatResponse(TaskServerHeartbeatCommand.ReportStatus) };
                }
                else
                {
                    lock( _topology )
                    {
                        // server.Rack might not be null if this is a dead data server that's re-reporting.
                        if( server.Rack == null )
                            _topology.AddNode(server);
                    }
                    _log.InfoFormat("Received initial status data for task server {0} in rack {1}.", address, server.Rack.RackId);
                    _taskServersModified = true;
                }
            }

            server.LastContactUtc = DateTime.UtcNow;

            List<JetHeartbeatResponse> responses = null;
            if( data != null )
            {
                foreach( JetHeartbeatData item in data )
                {
                    JetHeartbeatResponse response = ProcessHeartbeat(server, item);
                    if( response != null )
                    {
                        if( responses == null )
                            responses = new List<JetHeartbeatResponse>();
                        responses.Add(response);
                    }
                }
            }

            bool hasAvailableTasks;
            lock( _schedulerLock )
            {
                hasAvailableTasks = server.SchedulerInfo.AvailableTaskSlots > 0;
            }

            if( hasAvailableTasks )
            {
                // If this is a new task server and there are running jobs, we want to run the scheduler to see if we can assign any tasks to the new server.
                // At this point we know we've gotten the StatusHeartbeat because this function would've returned above if the new server didn't send one.
                if( _jobs.Values.Any(j => j.UnscheduledTaskCount > 0) )
                    RunScheduler();
            }

            lock( _schedulerLock )
            {
                if( server.SchedulerInfo.AssignedTasks.Count > 0 )
                {
                    var tasks = server.SchedulerInfo.AssignedTasks;
                    foreach( TaskInfo task in tasks )
                    {
                        if( task.State == TaskState.Scheduled )
                        {
                            if( responses == null )
                                responses = new List<JetHeartbeatResponse>();
                            ++task.SchedulerInfo.Attempts;
                            TaskAttemptId attemptId = new TaskAttemptId(task.TaskId, task.Attempts);
                            task.SchedulerInfo.CurrentAttempt = attemptId;
                            responses.Add(new RunTaskJetHeartbeatResponse(task.Job.Job, attemptId));
                            task.SchedulerInfo.State = TaskState.Running;
                            task.StartTimeUtc = DateTime.UtcNow;
                        }
                    }
                }
            }

            PerformCleanup(server, ref responses);

            return responses == null ? null : responses.ToArray();
        }

        #endregion

        private void PerformCleanup(TaskServerInfo server, ref List<JetHeartbeatResponse> responses)
        {
            lock( _jobsNeedingCleanup )
            {
                for( int x = 0; x < _jobsNeedingCleanup.Count; ++x )
                {
                    // Although we're accessing scheduler info, there's no need to take the scheduler lock because this job is in _jobsNeedingCleanup
                    JobInfo job = _jobsNeedingCleanup[x];
                    TaskServerJobInfo info = job.SchedulerInfo.GetTaskServer(server.Address);
                    if( info != null && info.NeedsCleanup )
                    {
                        job.CleanupServer(server);
                        _log.InfoFormat("Sending cleanup command for job {{{0}}} to server {1}.", job.Job.JobId, server.Address);
                        if( responses == null )
                            responses = new List<JetHeartbeatResponse>();
                        responses.Add(new CleanupJobJetHeartbeatResponse(job.Job.JobId));
                        info.NeedsCleanup = false;
                    }
                    if( !job.SchedulerInfo.NeedsCleanup )
                    {
                        _log.InfoFormat("Job {{{0}}} cleanup complete.", job.Job.JobId);
                        _jobsNeedingCleanup.RemoveAt(x);
                        --x;
                    }
                }
            }
        }

        private JetHeartbeatResponse ProcessHeartbeat(TaskServerInfo server, JetHeartbeatData data)
        {
            InitialStatusJetHeartbeatData statusData = data as InitialStatusJetHeartbeatData;
            if( statusData != null )
            {
                ProcessStatusHeartbeat(server, statusData);
                return null;
            }

            TaskStatusChangedJetHeartbeatData taskStatusChangedData = data as TaskStatusChangedJetHeartbeatData;
            if( taskStatusChangedData != null )
            {
                return ProcessTaskStatusChangedHeartbeat(server, taskStatusChangedData);
            }

            _log.WarnFormat("Task server {0} sent unknown heartbeat type {1}.", server.Address, data.GetType());
            throw new ArgumentException($"Unknown heartbeat type {data.GetType()}.");
        }

        private void ProcessStatusHeartbeat(TaskServerInfo server, InitialStatusJetHeartbeatData data)
        {
            if( server.HasReportedStatus )
            {
                _log.WarnFormat("Task server {0} re-reported initial status; it may have been restarted.", server.Address);
                lock( _schedulerLock )
                {
                    // We have to remove all tasks because if the server restarted it might not be running those anymore.
                    server.SchedulerInfo.UnassignAllTasks();
                }
            }

            server.HasReportedStatus = true;
            _log.InfoFormat("Task server {0} reported initial status: TaskSlots = {1}, FileServerPort = {2}", server.Address, data.TaskSlots, data.FileServerPort);
            server.TaskSlots = data.TaskSlots;
            server.FileServerPort = data.FileServerPort;

        }

        private JetHeartbeatResponse ProcessTaskStatusChangedHeartbeat(TaskServerInfo server, TaskStatusChangedJetHeartbeatData data)
        {
            if( data.Status >= TaskAttemptStatus.Running )
            {
                JobInfo job;
                if( !_jobs.TryGetValue(data.JobId, out job) )
                {
                    _log.WarnFormat("Task server {0} reported status for unknown job {1} (this may be the aftermath of a failed job).", server.Address, data.JobId);
                    if( data.Status == TaskAttemptStatus.Running )
                        return new KillTaskJetHeartbeatResponse(data.JobId, data.TaskAttemptId);
                    else
                        return null;
                }
                TaskInfo task = job.GetTask(data.TaskAttemptId.TaskId.ToString());


                if( task.Server != server || task.CurrentAttempt == null || task.CurrentAttempt.Attempt != data.TaskAttemptId.Attempt )
                {
                    _log.WarnFormat("Task server {0} reported status for task {{{1}}}_{2} which isn't an active attempt or was not assigned to that server.", server.Address, data.JobId, data.TaskAttemptId);
                    if( data.Status == TaskAttemptStatus.Running )
                        return new KillTaskJetHeartbeatResponse(data.JobId, data.TaskAttemptId);
                    else
                        return null;
                }

                if( data.Progress != null )
                {
                    if( task.State == TaskState.Running && data.Status != TaskAttemptStatus.Error )
                    {
                        task.Progress = data.Progress;
                        _log.InfoFormat("Task {0} reported progress: {1}", task.FullTaskId, data.Progress);
                    }
                }

                if( data.Metrics != null )
                {
                    task.Metrics = data.Metrics;
                }

                if( data.Status > TaskAttemptStatus.Running )
                {
                    // This access schedulerinfo in the task server info and various job and task state so must be done inside the scheduler lock
                    lock( _schedulerLock )
                    {
                        server.SchedulerInfo.AssignedTasks.Remove(task);
                        // We don't set task.Server to null because output tasks can still query that information!

                        switch( data.Status )
                        {
                        case TaskAttemptStatus.Completed:
                            task.EndTimeUtc = DateTime.UtcNow;
                            task.SchedulerInfo.CurrentAttempt = null;
                            task.SchedulerInfo.SuccessfulAttempt = data.TaskAttemptId;
                            task.SchedulerInfo.State = TaskState.Finished;
                            if( task.PartitionInfo != null )
                                task.PartitionInfo.FreezePartitions();
                            _log.InfoFormat("Task {0} completed successfully.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId));
                            if( task.Progress == null )
                                task.Progress = new TaskProgress() { Progress = 1.0f };
                            else
                                task.Progress.SetFinished();

                            ++job.SchedulerInfo.FinishedTasks;

                            if( _broadcaster != null )
                                _broadcaster.BroadcastTaskCompletion(data.JobId, data.TaskAttemptId, server);

                            break;
                        case TaskAttemptStatus.Error:
                            task.SchedulerInfo.CurrentAttempt = null;
                            task.SchedulerInfo.State = TaskState.Error;
                            if( task.PartitionInfo != null )
                                task.PartitionInfo.Reset();
                            _log.WarnFormat("Task {0} encountered an error.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId));
                            TaskStatus failedAttempt = task.ToTaskStatus();
                            task.Progress = null;
                            failedAttempt.EndTime = DateTime.UtcNow;
                            if( data.Progress != null )
                            {
                                // Task server sends a task progress containing the failure reason as the status message
                                // but no other data, so if we had other progress data we keep it.
                                if( failedAttempt.TaskProgress != null )
                                    failedAttempt.TaskProgress.StatusMessage = data.Progress.StatusMessage;
                                else
                                    failedAttempt.TaskProgress = data.Progress;
                            }
                            else
                            {
                                if( failedAttempt.TaskProgress == null )
                                    failedAttempt.TaskProgress = new TaskProgress();
                                failedAttempt.TaskProgress.StatusMessage = "Unknown failure reason.";
                            }
                            if( job.AddFailedTaskAttempt(failedAttempt) >= job.MaxTaskFailures )
                            {
                                job.FailureReason = string.Format(CultureInfo.InvariantCulture, "The job experienced the maximum of {0} task failures.", job.MaxTaskFailures);
                                _log.ErrorFormat("{0} Aborting the job.", job.FailureReason);
                                job.SchedulerInfo.State = JobState.Failed;
                            }
                            else if( task.Attempts < Configuration.JobServer.MaxTaskAttempts )
                            {
                                // Reschedule
                                task.Server.SchedulerInfo.UnassignFailedTask(task);
                                if( task.SchedulerInfo.BadServers.Count == _taskServers.Count )
                                    task.SchedulerInfo.BadServers.Clear(); // we've failed on all servers so try again anywhere.
                            }
                            else
                            {
                                job.FailureReason = string.Format(CultureInfo.InvariantCulture, "Task {0} failed more than {1} times.", Job.CreateFullTaskId(data.JobId, data.TaskAttemptId.TaskId), Configuration.JobServer.MaxTaskAttempts);
                                _log.ErrorFormat("{0} Aborting the job.", job.FailureReason);
                                job.SchedulerInfo.State = JobState.Failed;
                            }
                            ++job.SchedulerInfo.Errors;
                            break;
                        }

                        if( job.FinishedTaskCount == job.SchedulingTaskCount || job.State == JobState.Failed )
                        {
                            FinishOrFailJob(job);
                        }
                    } // lock( _schedulerLock )
                }
            }

            return null;
        }

        /// <summary>
        /// NOTE: Must be called inside the scheduler lock.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="job"></param>
        private void FinishOrFailJob(JobInfo job)
        {
            if( job.State != JobState.Failed )
            {
                _log.InfoFormat("Job {0}: all tasks in the job have finished.", job.Job.JobId);
                job.SchedulerInfo.State = JobState.Finished;
            }
            else
            {
                if( job.FailureReason == null )
                    job.FailureReason = "Unknown failure reason.";
                _log.ErrorFormat("Job {0} failed or was killed: {1}", job.Job.JobId, job.FailureReason);
                job.SchedulerInfo.AbortTasks();
            }

            JobInfo job2;
            _jobs.TryRemove(job.Job.JobId, out job2);
            lock( _orderedJobs )
            {
                _orderedJobs.Remove(job);
            }
            lock( _finishedJobs )
            {
                _finishedJobs.Add(job.Job.JobId, job);
            }
            lock( _jobsNeedingCleanup )
            {
                _jobsNeedingCleanup.Add(job);
            }

            job.EndTimeUtc = DateTime.UtcNow;

            ArchiveJob(job);
        }

        private void RunScheduler()
        {
            StartSchedulerThread();

            using( ManualResetEventSlim evt = new ManualResetEventSlim() )
            {
                _schedulerRequests.Add(evt, _cancellation.Token);
                if( !evt.Wait(_schedulerTimeoutMilliseconds, _cancellation.Token) )
                {
                    _log.Error("The scheduler timed out while waiting for scheduling run.");
                    lock( _schedulerThreadLock )
                    {
                        AbortSchedulerThread();

                        lock( _schedulerLock )
                        {
                            // TODO: Fail only the responsible job
                            List<JobInfo> jobs;
                            lock( _orderedJobs )
                            {
                                // Create a new List because FinishOrFailJob will remove items from _orderedJobs
                                jobs = new List<JobInfo>(_orderedJobs);
                            }

                            foreach( JobInfo job in jobs )
                            {
                                job.SchedulerInfo.State = JobState.Failed;
                                job.FailureReason = "Scheduler timed out.";
                                FinishOrFailJob(job);
                            }
                        }

                        // Restart the scheduler so that any requests in the queue will get signalled.
                        StartSchedulerThread();
                    }
                }
            }
        }

        private void StartSchedulerThread()
        {
            // Although running the scheduler on the thread pool might make sense, we don't want to do that
            // because the RunScheduler function itself might run on the thread pool, and it has to wait
            // for the scheduler to finish. A thread pool thread waiting for a thread pool thread can
            // lead to deadlock.
            lock( _schedulerThreadLock )
            {
                if( _schedulerThread == null )
                {
                    _schedulerCancellation = new();
                    _schedulerThread = new Thread(SchedulerThread) { Name = "SchedulerThread", IsBackground = true };
                    _schedulerThread.Start(_schedulerCancellation.Token);
                }
            }
        }

        private void AbortSchedulerThread()
        {
            lock( _schedulerThreadLock )
            {
                if( _schedulerThread != null )
                {
                    _schedulerCancellation.Cancel();
                    _schedulerCancellation = null;
                    _schedulerThread = null;
                }
            }
        }

        ///// <summary>
        ///// NOTE: Don't call inside the scheduler lock, will lead to deadlock.
        ///// </summary>
        ///// <param name="job"></param>
        //private void ScheduleTasks(JobInfo job)
        //{
        //    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
        //    sw.Start();
        //    lock( _schedulerJobQueue )
        //    {
        //        if( !_schedulerJobQueue.Contains(job) )
        //            _schedulerJobQueue.Enqueue(job);
        //        _schedulerWaitingEvent.Reset(); // We know that the scheduler queue contains items at this point and since we're holding the lock there's no way this can cross with the SchedulerThread
        //        Monitor.Pulse(_schedulerJobQueue);
        //    }

        //    lock( _schedulerThreadLock )
        //    {
        //        if( _schedulerThread == null )
        //        {
        //            _schedulerThread = new Thread(SchedulerThread) { Name = "SchedulerThread", IsBackground = true };
        //            _schedulerThread.Start();
        //        }
        //    }

        //    if( !_schedulerWaitingEvent.WaitOne(_schedulerTimeoutMilliseconds) )
        //    {
        //        // Scheduler timed out
        //        _log.ErrorFormat("The scheduler timed out while waiting for scheduling of job {{{0}}}.", job.Job.JobId);
        //        lock( _schedulerLock )
        //        {
        //            lock( _schedulerThreadLock )
        //            {
        //                _schedulerThread.Abort();
        //                _schedulerThread = null;
        //            }
        //            job.SchedulerInfo.State = JobState.Failed;
        //            job.FailureReason = "Scheduler timed out.";
        //            FinishOrFailJob(job);
        //        }
        //    }

        //    sw.Stop();
        //    _log.DebugFormat("Scheduling run took {0}", sw.Elapsed.TotalSeconds);
        //}

        private void ShutdownInternal()
        {
            _cancellation.Cancel();
            if( _broadcaster != null )
                _broadcaster.Dispose();
        }

        private JobInfo GetRunningOrFinishedJob(Guid jobId)
        {
            JobInfo job;
            if( !_jobs.TryGetValue(jobId, out job) )
            {
                lock( _finishedJobs )
                {
                    if( !_finishedJobs.TryGetValue(jobId, out job) )
                        throw new ArgumentException("Job not found.", nameof(jobId));
                }
            }
                
            return job;
        }

        private void SchedulerThread(object param)
        {
            var token = (CancellationToken)param;

            try
            {
                List<JobInfo> jobs = new List<JobInfo>();
                foreach( ManualResetEventSlim request in _schedulerRequests.GetConsumingEnumerable(_cancellation.Token) )
                {
                    try
                    {
                        DoSchedulingRun(jobs, token);
                    }
                    finally
                    {
                        request.Set();
                    }
                }
            }
            catch( OperationCanceledException )
            {
                _log.Info("Scheduler thread shut down due to cancellation.");
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Error should fail the job not the server.")]
        private void DoSchedulingRun(List<JobInfo> jobs, CancellationToken token)
        {
            lock( _orderedJobs )
            {
                jobs.AddRange(_orderedJobs);
            }

            lock( _schedulerLock )
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();

                foreach( JobInfo job in jobs )
                {
                    if( _taskServersModified || _taskServers.Count != job.SchedulerInfo.TaskServerCount )
                    {
                        foreach( TaskServerInfo server in _taskServers.Values )
                        {
                            job.SchedulerInfo.AddTaskServer(server);
                        }
                    }
                    _taskServersModified = false;
                }
                
                try
                {
                    _scheduler.ScheduleTasks(jobs, token);
                }
                catch( Exception ex )
                {
                    // TODO: Fail only the responsible job.
                    _log.Error("The scheduler encountered an error.", ex);
                    foreach( JobInfo job in jobs )
                    {
                        job.FailureReason = "The scheduler encountered an error.";
                        job.SchedulerInfo.State = JobState.Failed;
                        FinishOrFailJob(job);
                    }
                }

                sw.Stop();
                _log.DebugFormat("Scheduling run took {0}ms.", sw.ElapsedMilliseconds);
            }

            jobs.Clear();
        }

        private void CheckTaskServerTimeoutThread()
        {
            int timeout = Configuration.JobServer.TaskServerTimeout;
            int sleepTime = timeout / 3;

            while( !_cancellation.IsCancellationRequested )
            {
                List<TaskServerInfo> deadServers = null;
                Thread.Sleep(sleepTime);

                foreach( TaskServerInfo server in _taskServers.Values )
                {
                    if( server.HasReportedStatus && (DateTime.UtcNow - server.LastContactUtc).TotalMilliseconds > timeout )
                    {
                        if( deadServers == null )
                            deadServers = new List<TaskServerInfo>();
                        deadServers.Add(server);
                        server.HasReportedStatus = false;
                    }
                }

                if( deadServers != null )
                {
                    lock( _schedulerLock )
                    {
                        foreach( TaskServerInfo server in deadServers )
                        {
                            server.SchedulerInfo.UnassignAllTasks();
                        }
                    }
                }
            }
        }

        private void ArchiveJob(JobInfo job)
        {
            string archiveDir = Configuration.JobServer.ArchiveDirectory;
            if( !string.IsNullOrEmpty(archiveDir) )
            {
                Directory.CreateDirectory(archiveDir);
                string archiveFilePath = Path.Combine(archiveDir, _archiveFileName);
                JobStatus jobStatus = job.ToJobStatus();
                _log.InfoFormat("Archiving job {{{0}}}.", job.Job.JobId);

                lock( _archiveLock )
                {
                    using( FileStream stream = new FileStream(archiveFilePath, FileMode.Append, FileAccess.Write, FileShare.Read) )
                    using( BinaryRecordWriter<ArchivedJob> writer = new BinaryRecordWriter<ArchivedJob>(stream) )
                    {
                        writer.WriteRecord(new ArchivedJob(jobStatus));
                    }
                }

                _fileSystemClient.DownloadFile(job.Job.GetJobConfigurationFilePath(_fileSystemClient), Path.Combine(archiveDir, jobStatus.JobId + "_config.xml"));
                jobStatus.ToXml().Save(Path.Combine(archiveDir, jobStatus.JobId + "_summary.xml"));
            }
        }

        public void Dispose()
        {
            _broadcaster?.Dispose();
            _cancellation?.Dispose();
            _schedulerRequests?.Dispose();
        }
    }
}
