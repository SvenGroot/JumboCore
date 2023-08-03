// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Xml.Serialization;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about a particular stage.
    /// </summary>
    [Serializable]
    public class StageStatus
    {
        private readonly ExtendedCollection<TaskStatus> _tasks = new ExtendedCollection<TaskStatus>();
        private TaskMetrics? _stageMetrics; // Used only if the JobStatus was created with JobStatus.FromXml()

        /// <summary>
        /// Gets or sets the ID of the stage.
        /// </summary>
        public string? StageId { get; set; }

        /// <summary>
        /// Gets the tasks of this stage.
        /// </summary>
        public Collection<TaskStatus> Tasks
        {
            get { return _tasks; }
        }

        /// <summary>
        /// Gets the start time of this stage, or <see langword="null"/> if the stage hasn't started.
        /// </summary>
        [XmlIgnore]
        public DateTime? StartTime
        {
            get
            {
                return (from task in Tasks
                        where task.State >= TaskState.Running
                        select new DateTime?(task.StartTime)).Min();
            }
        }

        /// <summary>
        /// Gets a value that indicates whether all tasks in this stage have finished.
        /// </summary>
        [XmlIgnore]
        public bool IsFinished
        {
            get
            {
                return (from task in Tasks
                        where task.State != TaskState.Finished
                        select task).Count() == 0;
            }
        }

        /// <summary>
        /// Gets the end time of this stage, or <see langword="null"/> if the stage hasn't finished.
        /// </summary>
        [XmlIgnore]
        public DateTime? EndTime
        {
            get
            {
                if (IsFinished)
                {
                    return (from task in Tasks
                            select task.EndTime).Max();
                }
                else
                    return null;
            }
        }

        /// <summary>
        /// Gets the total progress of this stage.
        /// </summary>
        [XmlIgnore]
        public float Progress
        {
            get
            {
                return (from task in Tasks
                        select task.Progress).Average();
            }
        }

        /// <summary>
        /// Gets the total progress of this stage, including additional progress values.
        /// </summary>
        /// <value>The stage progress.</value>
        [XmlIgnore]
        public TaskProgress StageProgress
        {
            get
            {
                var result = new TaskProgress();
                foreach (var task in Tasks)
                {
                    if (task.TaskProgress != null)
                    {
                        result.Progress += task.TaskProgress.Progress;
                        if (task.TaskProgress.AdditionalProgressValues != null)
                        {
                            if (result.AdditionalProgressValues == null)
                            {
                                foreach (var value in task.TaskProgress.AdditionalProgressValues)
                                    result.AddAdditionalProgressValue(value.SourceName, value.Progress);
                            }
                            else
                            {
                                for (var x = 0; x < result.AdditionalProgressValues.Count; ++x)
                                    result.AdditionalProgressValues[x].Progress += task.TaskProgress.AdditionalProgressValues[x].Progress;
                            }
                        }
                        else if (result.AdditionalProgressValues != null && task.TaskProgress.OverallProgress >= 1.0f)
                        {
                            foreach (var value in result.AdditionalProgressValues)
                                value.Progress += 1.0f;
                        }
                    }
                }

                result.Progress /= Tasks.Count;
                if (result.AdditionalProgressValues != null)
                {
                    foreach (var value in result.AdditionalProgressValues)
                        value.Progress /= Tasks.Count;
                }

                return result;
            }
        }

        /// <summary>
        /// Gets the combined metrics for all the tasks in the stage.
        /// </summary>
        /// <value>The metrics.</value>
        [XmlIgnore]
        public TaskMetrics Metrics
        {
            get
            {
                // Stage metrics will be non-null only if the job was loaded with JobStatus.FromXml().
                if (_stageMetrics != null)
                    return _stageMetrics;

                var result = new TaskMetrics();
                foreach (var task in Tasks)
                {
                    var metrics = task.Metrics;
                    if (metrics != null)
                    {
                        result.Add(metrics);
                    }
                }
                return result;
            }
            internal set
            {
                _stageMetrics = value;
            }
        }

        /// <summary>
        /// Gets the number of running tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int RunningTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State == TaskState.Running
                        select task).Count();
            }
        }

        /// <summary>
        /// Gets the number of pending tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int PendingTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State < TaskState.Running
                        select task).Count();
            }
        }

        /// <summary>
        /// Gets the number of finished tasks in this stage.
        /// </summary>
        [XmlIgnore]
        public int FinishedTaskCount
        {
            get
            {
                return (from task in Tasks
                        where task.State == TaskState.Finished
                        select task).Count();
            }
        }
    }
}
