// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Scheduling;

namespace JobServerApplication
{
    enum JobState
    {
        Running,
        Finished,
        Failed
    }

    /// <summary>
    /// Information about a running, finished or failed job. All mutable properties of this class may be read without locking, but must be set only inside the scheduler lock. Access the <see cref="TaskServers"/>
    /// property only inside the scheduler lock. For modifying any <see cref="TaskInfo"/> instances belonging to that job refer to the locking rules for that class.
    /// </summary>
    class JobInfo : IJobInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JobInfo));

        private readonly Dictionary<string, TaskInfo> _schedulingTasksById = new Dictionary<string, TaskInfo>();
        private readonly ReadOnlyCollection<StageInfo> _stages;
        private readonly Job _job;
        private readonly DateTime _startTimeUtc;
        private readonly string _jobName;
        private readonly JobConfiguration _config;
        private readonly JobSchedulerInfo _schedulerInfo;
        private readonly int _maxTaskFailures;

        private long _endTimeUtcTicks;
        private volatile List<TaskStatus> _failedTaskAttempts;

        public JobInfo(Job job, JobConfiguration config, FileSystemClient fileSystem)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (fileSystem == null)
                throw new ArgumentNullException(nameof(fileSystem));
            _job = job;
            _config = config;

            _jobName = config.JobName;
            _maxTaskFailures = JobServer.Instance.Configuration.JobServer.MaxTaskFailures;

            var stages = new List<StageInfo>();
            _stages = stages.AsReadOnly();
            foreach (var stage in config.GetDependencyOrderedStages())
            {
                if (stage.TaskCount < 1)
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Stage {0} has no tasks.", stage.StageId), nameof(config));
                // Don't allow failures for a job with a TCP channel.
                if (stage.Leaf.OutputChannel != null && stage.Leaf.OutputChannel.ChannelType == Ookii.Jumbo.Jet.Channels.ChannelType.Tcp)
                    _maxTaskFailures = 1;
                var nonDataInputStage = !stage.HasDataInput;
                // Don't do the work trying to find the input stages if the stage has data inputs.
                var inputStages = nonDataInputStage ? config.GetInputStagesForStage(stage.StageId).ToArray() : null;
                var stageInfo = new StageInfo(this, stage);
                var inputLocations = nonDataInputStage ? null : TaskInputUtility.ReadTaskInputLocations(fileSystem, job.Path, stage.StageId);
                if (inputLocations != null && inputLocations.Count != stage.TaskCount)
                    throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "The number of input splits for stage {0} doesn't match the stage's task count.", stage.StageId));
                for (var x = 1; x <= stage.TaskCount; ++x)
                {
                    TaskInfo taskInfo;

                    taskInfo = new TaskInfo(this, stageInfo, inputStages, x, nonDataInputStage ? null : inputLocations[x - 1]);
                    _schedulingTasksById.Add(taskInfo.TaskId.ToString(), taskInfo);

                    stageInfo.Tasks.Add(taskInfo);
                }
                stages.Add(stageInfo);
            }

            // This must be done afterwards because stages using TCP channels can appear in the depenency ordered list in reverse order
            // and we must be sure both are already in the list before soft dependencies can be set up.
            foreach (var stage in stages)
                stage.SetupSoftDependencies(this);

            if (stages.Count == 0)
                throw new ArgumentException("The job configuration has no stages.", nameof(config));

            if (_config.SchedulerOptions.DataInputSchedulingMode == SchedulingMode.Default)
                _config.SchedulerOptions.DataInputSchedulingMode = JobServer.Instance.Configuration.JobServer.DataInputSchedulingMode;
            if (_config.SchedulerOptions.NonDataInputSchedulingMode == SchedulingMode.Default || _config.SchedulerOptions.NonDataInputSchedulingMode == SchedulingMode.OptimalLocality)
                _config.SchedulerOptions.NonDataInputSchedulingMode = JobServer.Instance.Configuration.JobServer.NonDataInputSchedulingMode;

            _log.InfoFormat("Job {0:B} is using data input scheduling mode {1} and non-data input scheduling mode {1}.", job.JobId, _config.SchedulerOptions.DataInputSchedulingMode, _config.SchedulerOptions.NonDataInputSchedulingMode);

            _startTimeUtc = DateTime.UtcNow;
            _schedulerInfo = new JobSchedulerInfo(this)
            {
                UnscheduledTasks = _schedulingTasksById.Count,
                State = JobState.Running
            };
        }

        /// <summary>
        /// Only access inside scheduler lock.
        /// </summary>
        public JobSchedulerInfo SchedulerInfo
        {
            get { return _schedulerInfo; }
        }

        public Job Job
        {
            get { return _job; }
        }

        public string JobName
        {
            get { return _jobName; }
        }

        public JobConfiguration Configuration
        {
            get { return _config; }
        }

        public DateTime StartTimeUtc
        {
            get { return _startTimeUtc; }
        }

        public JobState State
        {
            get { return _schedulerInfo.State; }
        }

        public int UnscheduledTaskCount
        {
            get { return _schedulerInfo.UnscheduledTasks; }
        }

        public int FinishedTaskCount
        {
            get { return _schedulerInfo.FinishedTasks; }
        }

        public int ErrorCount
        {
            get { return _schedulerInfo.Errors; }
        }

        public string FailureReason { get; set; }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public ReadOnlyCollection<StageInfo> Stages
        {
            get { return _stages; }
        }

        public int MaxTaskFailures
        {
            get { return _maxTaskFailures; }
        }

        public int SchedulingTaskCount
        {
            get { return _schedulingTasksById.Count; }
        }

        public int RunningTaskCount
        {
            get
            {
                return (from task in _schedulingTasksById.Values
                        where task.State == TaskState.Running
                        select task).Count();
            }
        }

        public TaskInfo GetTask(string taskId)
        {
            return _schedulingTasksById[taskId];
        }

        public StageInfo GetStage(string stageId)
        {
            return _stages.Where(stage => stage.StageId == stageId).SingleOrDefault();
        }

        /// <summary>
        /// Removes assigned tasks from this job from the task server. Job must be waiting for cleanup.
        /// </summary>
        /// <param name="server"></param>
        public void CleanupServer(TaskServerInfo server)
        {
            foreach (var task in _schedulingTasksById.Values)
            {
                // No need to use the scheduler lock for a job in _jobsNeedingCleanup
                server.SchedulerInfo.AssignedTasks.Remove(task);
            }
        }

        /// <summary>
        /// Adds a failed task attempt. Doesn't need any locking (because it does its own so that ToJobStatus can be called without locking).
        /// </summary>
        /// <param name="failedTaskAttempt"></param>
        public int AddFailedTaskAttempt(TaskStatus failedTaskAttempt)
        {
#pragma warning disable 420 // volatile field not treated as volatile warning

            if (_failedTaskAttempts == null)
                Interlocked.CompareExchange(ref _failedTaskAttempts, new List<TaskStatus>(), null);

#pragma warning restore 420

            lock (_failedTaskAttempts)
            {
                _failedTaskAttempts.Add(failedTaskAttempt);
                return _failedTaskAttempts.Count;
            }
        }

        public JobStatus ToJobStatus()
        {
            var result = new JobStatus()
            {
                JobId = Job.JobId,
                JobName = JobName,
                IsFinished = State > JobState.Running,
                RunningTaskCount = RunningTaskCount,
                UnscheduledTaskCount = UnscheduledTaskCount,
                FinishedTaskCount = FinishedTaskCount,
                StartTime = StartTimeUtc,
                EndTime = EndTimeUtc,
                FailureReason = FailureReason
            };
            result.Stages.AddRange(from stage in Stages select stage.ToStageStatus());
            if (_failedTaskAttempts != null)
            {
                lock (_failedTaskAttempts)
                {
                    result.FailedTaskAttempts.AddRange(_failedTaskAttempts);
                }
            }
            if (_config.AdditionalProgressCounters != null)
            {
                result.AdditionalProgressCounters.AddRange(_config.AdditionalProgressCounters);
            }
            return result;
        }

        Guid IJobInfo.JobId
        {
            get { return Job.JobId; }
        }

        IEnumerable<IStageInfo> IJobInfo.Stages
        {
            get { return _stages; }
        }

        IEnumerable<ITaskServerJobInfo> IJobInfo.TaskServers
        {
            get { return SchedulerInfo.TaskServers; }
        }
    }
}
