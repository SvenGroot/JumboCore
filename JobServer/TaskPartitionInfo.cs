// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Jobs;

namespace JobServerApplication
{
    sealed class TaskPartitionInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskPartitionInfo));

        private readonly TaskInfo _task;
        private readonly List<int> _partitions;
        private readonly HashSet<int> _unstartedPartitions = new HashSet<int>();
        private volatile bool _frozen;

        public TaskPartitionInfo(TaskInfo task, IList<StageConfiguration> inputStages)
        {
            _task = task;

            foreach (StageConfiguration inputStage in inputStages)
            {
                if (_partitions == null)
                {
                    int partitionsPerTask = inputStage.OutputChannel.PartitionsPerTask;
                    _partitions = new List<int>(partitionsPerTask < 1 ? 1 : partitionsPerTask);
                    if (partitionsPerTask <= 1)
                        _partitions.Add(task.TaskId.TaskNumber);
                    else
                    {
                        if (inputStage.OutputChannel.PartitionAssignmentMethod == PartitionAssignmentMethod.Striped)
                        {
                            int partition = task.TaskId.TaskNumber;
                            for (int x = 0; x < partitionsPerTask; ++x, partition += task.Stage.Configuration.TaskCount)
                            {
                                _partitions.Add(partition);
                            }
                        }
                        else
                        {
                            int begin = ((task.TaskId.TaskNumber - 1) * partitionsPerTask) + 1;
                            _partitions.AddRange(Enumerable.Range(begin, partitionsPerTask));
                        }
                    }
                }
                else if (inputStage.OutputChannel.PartitionsPerTask > 1 || _partitions.Count > 1)
                    throw new InvalidOperationException("Cannot use multiple partitions per task when there are multiple input channels.");
            }

            Reset();

            _log.InfoFormat("Task {0} has been assigned the following partitions: {1}", task.FullTaskId, _partitions.ToDelimitedString());
        }

        public int UnstartedPartitionCount
        {
            get
            {
                lock (_partitions)
                {
                    return _unstartedPartitions.Count;
                }
            }
        }

        public int[] GetAssignedPartitions()
        {
            lock (_partitions)
            {
                return _partitions.ToArray();
            }
        }

        public bool NotifyStartPartitionProcessing(int partition)
        {
            lock (_partitions)
            {
                if (!_partitions.Contains(partition))
                    return false; // We're not assigned this partitions. It may have been re-assigned after calling GetAssignedPartitions

                _unstartedPartitions.Remove(partition);
            }
            _log.DebugFormat("Task {0} has started processing partition {1}.", _task.FullTaskId, partition);
            return true;
        }

        public int AssignAdditionalPartition()
        {
            int additionalPartition = -1;
            // This lock is so two tasks in the same stage won't do this at the same time, so they won't pick the same task.
            lock (_task.Stage)
            {
                // If the assigned partitions are frozen, always return -1.
                if (!_frozen)
                {
                    TaskInfo candidateTask = GetTaskWithMostUnstartedPartitions();
                    if (candidateTask != null)
                    {
                        additionalPartition = candidateTask.PartitionInfo.UnassignLastPartition();
                        if (additionalPartition != -1)
                        {
                            lock (_partitions)
                            {
                                // We don't add this partition to unstarted partitions because we automatically consider it started.
                                _partitions.Add(additionalPartition);
                            }
                            _log.DebugFormat("Partition {0} has been reassigned from task {1} to task {2}.", additionalPartition, candidateTask.FullTaskId, _task.FullTaskId);
                        }
                    }
                }
            }

            return additionalPartition;
        }

        public void FreezePartitions()
        {
            // This is used once a task is finished, so that if it needs to be re-executed (because some other task couldn't download this task's output)
            // it's guaranteed to process the same partitions on the re-execution.
            _frozen = true;
        }

        public void Reset()
        {
            lock (_partitions)
            {
                // TODO: If we want to reset _partitions to its initial state too, we need to have a way to keep track of partitions that aren't assigned to anyone at all.
                _unstartedPartitions.Clear();
                // We always consider the first partition as started, so we never reassign it.
                foreach (int partition in _partitions.Skip(1))
                    _unstartedPartitions.Add(partition);
            }
        }

        private int UnassignLastPartition()
        {
            lock (_partitions)
            {
                int lastPartition = _partitions[_partitions.Count - 1];
                if (!_unstartedPartitions.Remove(lastPartition))
                {
                    // Last partition already started
                    return -1;
                }

                _partitions.RemoveAt(_partitions.Count - 1);
                return lastPartition;
            }
        }

        private TaskInfo GetTaskWithMostUnstartedPartitions()
        {
            TaskInfo result = null;
            foreach (TaskInfo task in _task.Stage.Tasks)
            {
                if (task != _task && !task.PartitionInfo._frozen)
                {
                    int unstartedPartitionCount = task.PartitionInfo.UnstartedPartitionCount;
                    if (unstartedPartitionCount > 0 && (result == null || unstartedPartitionCount > result.PartitionInfo.UnstartedPartitionCount))
                    {
                        result = task;
                    }
                }
            }

            return result;
        }
    }
}
