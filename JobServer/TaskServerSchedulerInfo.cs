// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet;

namespace JobServerApplication
{
    /// <summary>
    /// Information about a task server that can be modified by the scheduler. Only access the properties of this class inside the scheduler lock!
    /// </summary>
    sealed class TaskServerSchedulerInfo
    {
        private readonly TaskServerInfo _taskServer;
        private readonly List<TaskInfo> _assignedTasks = new List<TaskInfo>();

        public TaskServerSchedulerInfo(TaskServerInfo taskServer)
        {
            _taskServer = taskServer;
        }

        public int AvailableTaskSlots
        {
            get { return _taskServer.TaskSlots - _assignedTasks.Count; }
        }

        public List<TaskInfo> AssignedTasks
        {
            get { return _assignedTasks; }
        }

        public void AssignTask(JobInfo job, TaskInfo task)
        {
            AssignedTasks.Add(task);
            task.SchedulerInfo.Server = _taskServer;
            task.SchedulerInfo.State = TaskState.Scheduled;
            --job.SchedulerInfo.UnscheduledTasks;
            job.SchedulerInfo.GetTaskServer(_taskServer.Address).NeedsCleanup = true;
        }

        public void UnassignFailedTask(TaskInfo task)
        {
            // This is used if a task has failed and needs to be rescheduled.
            AssignedTasks.Remove(task);
            task.SchedulerInfo.Server = null;
            task.SchedulerInfo.BadServers.Add(_taskServer);
            task.SchedulerInfo.State = TaskState.Created;
            ++task.Job.SchedulerInfo.UnscheduledTasks;
        }

        public void UnassignAllTasks()
        {
            // This is used if a task server is restarted.
            foreach (TaskInfo task in AssignedTasks)
            {
                task.SchedulerInfo.Server = null;
                task.SchedulerInfo.BadServers.Add(_taskServer);
                task.SchedulerInfo.State = TaskState.Created;
                ++task.SchedulerInfo.Attempts;
                ++task.Job.SchedulerInfo.UnscheduledTasks;
            }

            AssignedTasks.Clear();
        }
    }
}
