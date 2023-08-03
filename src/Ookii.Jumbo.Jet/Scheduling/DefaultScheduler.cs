// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Ookii.Jumbo.Jet.Scheduling
{
    /// <summary>
    /// The default Jumbo Jet task scheduler.
    /// </summary>
    /// <remarks>
    /// This scheduler schedules jobs in FIFO order, and schedules stages in dependency order.
    /// </remarks>
    public class DefaultScheduler : ITaskScheduler
    {
        #region Nested types

        private sealed class TaskServerSlotsComparer : IComparer<ITaskServerJobInfo>
        {
            public bool Invert { get; set; }

            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
            public int Compare(ITaskServerJobInfo? x, ITaskServerJobInfo? y)
            {
                Debug.Assert(x != null && y != null);

                if (x.AvailableTaskSlots < y.AvailableTaskSlots)
                    return Invert ? 1 : -1;
                else if (x.AvailableTaskSlots > y.AvailableTaskSlots)
                    return Invert ? -1 : 1;
                else
                    return 0;
            }
        }

        private sealed class TaskServerLocalTasksComparer : IComparer<ITaskServerJobInfo>
        {
            private readonly IStageInfo _stage;

            public TaskServerLocalTasksComparer(IStageInfo stage)
            {
                _stage = stage;
            }

            [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
            public int Compare(ITaskServerJobInfo? x, ITaskServerJobInfo? y)
            {
                Debug.Assert(x != null && y != null);
                if (x == y)
                    return 0;

                var tasksX = x.GetLocalTaskCount(_stage);
                var tasksY = y.GetLocalTaskCount(_stage);

                if (tasksX < tasksY)
                    return -1;
                else if (tasksX > tasksY)
                    return 1;
                else
                    return 0;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DefaultScheduler));

        /// <summary>
        /// Performs a scheduling pass.
        /// </summary>
        /// <param name="jobs">The current active jobs.</param>
        /// <param name="token">The cancellation token to observe.</param>
        public void ScheduleTasks(IEnumerable<IJobInfo> jobs, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(jobs);
            // Schedule with increasing data distance or until we run out of capacity or tasks
            // If the cluster has only one rack, distance 1 is the same as distance 2, and the cluster must run out of either tasks or capacity
            // for distance 1 so there's no need to check and short-circuit the loop.
            foreach (var job in jobs)
            {
                token.ThrowIfCancellationRequested();
                if (!ScheduleJob(job, token))
                    break;
            }
        }

        private static bool ScheduleJob(IJobInfo job, CancellationToken token)
        {
            foreach (var stage in job.Stages)
            {
                token.ThrowIfCancellationRequested();
                if (stage.IsReadyForScheduling && stage.UnscheduledTaskCount > 0)
                {
                    if (stage.Configuration.HasDataInput)
                    {
                        // ScheduleNonDataInputTasks returns false if there is no more cluster capacity left.
                        if (!ScheduleDataInputTasks(job, stage, token))
                            return false;
                    }
                    else
                    {
                        // ScheduleNonDataInputTasks returns false if there is no more cluster capacity left.
                        if (!ScheduleNonDataInputTasks(job, stage, token))
                            return false;
                    }
                }
            }

            return true;
        }

        private static bool ScheduleDataInputTasks(IJobInfo job, IStageInfo stage, CancellationToken token)
        {
            IComparer<ITaskServerJobInfo> comparer;

            switch (job.Configuration.SchedulerOptions.DataInputSchedulingMode)
            {
            case SchedulingMode.FewerServers:
                comparer = new TaskServerSlotsComparer() { Invert = false };
                break;
            case SchedulingMode.OptimalLocality:
                comparer = new TaskServerLocalTasksComparer(stage);
                break;
            default:
                // If spreading we want high amounts of available tasks at the front of the queue.
                comparer = new TaskServerSlotsComparer() { Invert = true };
                break;
            }


            var tasksAndCapacityLeft = true;
            for (var distance = 0; distance < 3 && distance <= job.Configuration.SchedulerOptions.MaximumDataDistance && tasksAndCapacityLeft; ++distance)
            {
                var availableTaskServers = job.TaskServers.Where(server => server.IsActive && server.AvailableTaskSlots > 0);
                var taskServers = new PriorityQueue<ITaskServerJobInfo>(availableTaskServers, comparer);
                tasksAndCapacityLeft = ScheduleDataInputTasks(taskServers, stage, distance, token);
            }

            return job.TaskServers.Any(server => server.IsActive && server.AvailableTaskSlots > 0);
        }

        private static bool ScheduleDataInputTasks(PriorityQueue<ITaskServerJobInfo> taskServers, IStageInfo stage, int distance, CancellationToken token)
        {
            var unscheduledTasks = stage.UnscheduledTaskCount; // Tasks that can be scheduled but haven't been scheduled yet.
            var capacityRemaining = false;

            if (unscheduledTasks > 0)
            {
                while (taskServers.Count > 0 && unscheduledTasks > 0)
                {
                    token.ThrowIfCancellationRequested();
                    var server = taskServers.Peek();
                    var task = server.FindDataInputTaskToSchedule(stage, distance);
                    if (task != null)
                    {
                        server.AssignTask(task, distance);
                        --unscheduledTasks;

                        _log.InfoFormat("Task {0} has been assigned to server {1} ({2}).", task.FullTaskId, server.Address, distance < 0 ? "no locality data available" : (distance == 0 ? "data local" : (distance == 1 ? "rack local" : "NOT data local")));
                        if (server.AvailableTaskSlots == 0)
                            taskServers.Dequeue(); // No more available tasks, remove it from the queue
                        else
                            taskServers.AdjustFirstItem(); // Available tasks changed so re-evaluate its position in the queue.
                    }
                    else
                    {
                        capacityRemaining = true; // Indicate that we removed a task server from the queue that still has capacity left.
                        taskServers.Dequeue(); // If there's no task we can schedule on this server, remove it from the queue.
                    }
                }
            }

            // Return true if there's task left to schedule, and capacity where they can be scheduled.
            return unscheduledTasks > 0 && capacityRemaining;
        }

        private static bool ScheduleNonDataInputTasks(IJobInfo job, IStageInfo stage, CancellationToken token)
        {
            var unscheduledTasks = stage.Tasks.Where(t => !t.IsAssignedToServer).ToList();
            Debug.Assert(unscheduledTasks.Count > 0);
            var availableTaskServers = job.TaskServers.Where(server => server.IsActive && server.AvailableTaskSlots > 0);

            var comparer = new TaskServerSlotsComparer();
            comparer.Invert = job.Configuration.SchedulerOptions.NonDataInputSchedulingMode != SchedulingMode.FewerServers;
            var taskServers = new PriorityQueue<ITaskServerJobInfo>(availableTaskServers, comparer);

            while (taskServers.Count > 0 && unscheduledTasks.Count > 0)
            {
                token.ThrowIfCancellationRequested();
                var server = taskServers.Peek();
                // We search backwards because that will make the remove operation cheaper.
                var taskIndex = unscheduledTasks.FindLastIndex(task => !task.IsBadServer(server));
                if (taskIndex >= 0)
                {
                    // Found a task we can schedule.
                    var task = unscheduledTasks[taskIndex];
                    unscheduledTasks.RemoveAt(taskIndex);
                    server.AssignTask(task);
                    _log.InfoFormat("Task {0} has been assigned to server {1}.", task.FullTaskId, server.Address);
                    if (server.AvailableTaskSlots == 0)
                        taskServers.Dequeue(); // No more available tasks, remove it from the queue
                    else
                        taskServers.AdjustFirstItem(); // Available tasks changed so re-evaluate its position in the queue.
                }
                else
                    taskServers.Dequeue(); // If there's no task we can schedule on this server, remove it from the queue.
            }
            return taskServers.Count > 0;
        }
    }
}
