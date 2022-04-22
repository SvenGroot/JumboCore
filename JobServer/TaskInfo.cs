// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Threading;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Scheduling;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task of a running job.
    /// </summary>
    sealed class TaskInfo : ITaskInfo
    {
        private readonly StageInfo _stage;
        private readonly TaskId _taskId;
        private readonly string _fullTaskId;
        private readonly JobInfo _job;
        private readonly TaskPartitionInfo _partitionInfo;
        private readonly TaskSchedulerInfo _schedulerInfo;
        private readonly string[] _inputLocations;

        private long _startTimeUtcTicks;
        private long _endTimeUtcTicks;

        public TaskInfo(JobInfo job, StageInfo stage, IList<StageConfiguration> inputStages, int taskNumber, string[] inputLocations)
        {
            if (stage == null)
                throw new ArgumentNullException(nameof(stage));
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            _stage = stage;
            _taskId = new TaskId(stage.StageId, taskNumber);
            _fullTaskId = Ookii.Jumbo.Jet.Job.CreateFullTaskId(job.Job.JobId, _taskId);
            _job = job;
            _inputLocations = inputLocations;

            if (inputStages != null && inputStages.Count > 0)
            {
                _partitionInfo = new TaskPartitionInfo(this, inputStages);
            }

            _schedulerInfo = new TaskSchedulerInfo(this);
        }

        public bool IsLocalForHost(string hostName)
        {
            return _inputLocations != null && Array.IndexOf(_inputLocations, hostName) >= 0;
        }

        public StageInfo Stage
        {
            get { return _stage; }
        }

        public TaskId TaskId
        {
            get { return _taskId; }
        }

        public JobInfo Job
        {
            get { return _job; }
        }

        // Do not access except inside the scheduler lock.
        public TaskSchedulerInfo SchedulerInfo
        {
            get { return _schedulerInfo; }
        }

        public TaskPartitionInfo PartitionInfo
        {
            get { return _partitionInfo; }
        }

        public string[] InputLocations
        {
            get { return _inputLocations; }
        }

        public TaskState State
        {
            get
            {
                return _schedulerInfo.State;
            }
        }

        public TaskServerInfo Server
        {
            get
            {
                return _schedulerInfo.Server;
            }
        }

        public TaskAttemptId CurrentAttempt
        {
            get
            {
                return _schedulerInfo.CurrentAttempt;
            }
        }

        public TaskAttemptId SuccessfulAttempt
        {
            get
            {
                return _schedulerInfo.SuccessfulAttempt;
            }
        }

        public int CurrentAttemptDataDistance
        {
            get { return _schedulerInfo.CurrentAttemptDataDistance; }
        }

        public DateTime StartTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _startTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _startTimeUtcTicks, value.Ticks); }
        }

        public DateTime EndTimeUtc
        {
            get { return new DateTime(Interlocked.Read(ref _endTimeUtcTicks), DateTimeKind.Utc); }
            set { Interlocked.Exchange(ref _endTimeUtcTicks, value.Ticks); }
        }

        public TaskProgress Progress { get; set; }

        public TaskMetrics Metrics { get; set; }

        public int Attempts
        {
            get
            {
                return _schedulerInfo.Attempts;
            }
        }

        public string FullTaskId
        {
            get
            {
                return _fullTaskId;
            }
        }

        public TaskStatus ToTaskStatus()
        {
            // making a local copy of stuff we need more than once for thread safety.
            var server = Server;
            var startTimeUtc = StartTimeUtc;
            return new TaskStatus()
            {
                TaskId = TaskId.ToString(),
                State = State,
                TaskServer = server == null ? null : server.Address,
                Attempts = Attempts,
                StartTime = startTimeUtc,
                EndTime = EndTimeUtc,
                StartOffset = startTimeUtc - _job.StartTimeUtc,
                TaskProgress = Progress,
                Metrics = Metrics,
                DataDistance = CurrentAttemptDataDistance
            };
        }


        public bool IsAssignedToServer
        {
            get { return Server != null; }
        }

        // Explicitly implemented because it requires scheduler lock
        bool ITaskInfo.IsBadServer(ITaskServerJobInfo server)
        {
            return SchedulerInfo.BadServers.Contains(((TaskServerJobInfo)server).TaskServer);
        }
    }
}
