// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet.Scheduling;

namespace JobServerApplication
{
    /// <summary>
    /// Stores data that is related to a specific task server and a specific job. All members of this class should only be accessed inside the scheduler lock.
    /// </summary>
    sealed class TaskServerJobInfo : ITaskServerJobInfo
    {
        private readonly TaskServerInfo _taskServer;
        private readonly JobInfo _job;
        private List<TaskInfo> _localTasks;
        private List<TaskInfo> _rackLocalTasks;

        public TaskServerJobInfo(TaskServerInfo taskServer, JobInfo job)
        {
            ArgumentNullException.ThrowIfNull(taskServer);
            ArgumentNullException.ThrowIfNull(job);
            _taskServer = taskServer;
            _job = job;
        }

        public TaskServerInfo TaskServer
        {
            get { return _taskServer; }
        }

        public bool NeedsCleanup { get; set; }

        public int GetSchedulableLocalTaskCount()
        {
            return (from task in GetLocalTasks()
                    where task.Stage.IsReadyForScheduling && task.Server == null && !task.SchedulerInfo.BadServers.Contains(_taskServer)
                    select task).Count();
        }

        private List<TaskInfo> GetLocalTasks()
        {
            if (_localTasks == null)
                _localTasks = CreateLocalTaskList();

            return _localTasks;
        }

        private List<TaskInfo> GetRackLocalTasks()
        {
            if (_rackLocalTasks == null)
            {
                _rackLocalTasks = _job.SchedulerInfo.GetRackTasks(_taskServer.Rack.RackId);
                if (_rackLocalTasks == null)
                {
                    _rackLocalTasks = CreateRackLocalTaskList();
                    _job.SchedulerInfo.AddRackTasks(_taskServer.Rack.RackId, _rackLocalTasks);
                }
            }

            return _rackLocalTasks;
        }

        private List<TaskInfo> CreateLocalTaskList()
        {
            return (from stage in _job.Stages
                    where stage.Configuration.HasDataInput
                    from task in stage.Tasks
                    where task.IsLocalForHost(_taskServer.Address.HostName)
                    select task).ToList();
        }

        private List<TaskInfo> CreateRackLocalTaskList()
        {
            var taskServers = _taskServer.Rack.Nodes.Cast<TaskServerInfo>();
            return (from stage in _job.Stages
                    where stage.Configuration.HasDataInput
                    from task in stage.Tasks
                    where taskServers.Any(server => task.IsLocalForHost(server.Address.HostName))
                    select task).ToList();
        }

        ServerAddress ITaskServerJobInfo.Address
        {
            get { return TaskServer.Address; }
        }

        bool ITaskServerJobInfo.IsActive
        {
            get { return TaskServer.IsActive; }
        }

        int ITaskServerJobInfo.AvailableTaskSlots
        {
            get { return TaskServer.SchedulerInfo.AvailableTaskSlots; }
        }

        ITaskInfo ITaskServerJobInfo.FindDataInputTaskToSchedule(IStageInfo stage, int distance)
        {
            ArgumentNullException.ThrowIfNull(stage);
            if (!stage.Configuration.HasDataInput)
                throw new ArgumentException("Stage does not have data input.", nameof(stage));
            if (!stage.IsReadyForScheduling)
                return null;

            IEnumerable<ITaskInfo> eligibleTasks;
            switch (distance)
            {
            case 0:
                eligibleTasks = GetLocalTasks().Where(task => task.Stage == stage);
                break;
            case 1:
                if (JobServer.Instance.RackCount > 1)
                    eligibleTasks = GetRackLocalTasks().Where(task => task.Stage == stage);
                else
                    eligibleTasks = stage.Tasks;
                break;
            default:
                eligibleTasks = stage.Tasks;
                break;
            }

            return (from task in eligibleTasks
                    where !task.IsAssignedToServer && !task.IsBadServer(this)
                    select task).FirstOrDefault();
        }

        void ITaskServerJobInfo.AssignTask(ITaskInfo task, int? dataDistance)
        {
            ArgumentNullException.ThrowIfNull(task);
            var taskInfo = (TaskInfo)task;
            TaskServer.SchedulerInfo.AssignTask(_job, taskInfo);

            if (dataDistance != null)
                taskInfo.SchedulerInfo.CurrentAttemptDataDistance = dataDistance.Value;
        }


        int ITaskServerJobInfo.GetLocalTaskCount(IStageInfo stage)
        {
            ArgumentNullException.ThrowIfNull(stage);
            return (from task in GetLocalTasks()
                    where task.Stage == stage && !task.SchedulerInfo.BadServers.Contains(_taskServer)
                    select task).Count();
        }
    }
}
