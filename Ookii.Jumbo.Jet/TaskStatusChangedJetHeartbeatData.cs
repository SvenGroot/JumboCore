// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat data used to inform the job server that the status of a task has changed.
    /// </summary>
    [Serializable]
    public class TaskStatusChangedJetHeartbeatData : JetHeartbeatData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TaskStatusChangedJetHeartbeatData"/> class.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskAttemptId">The task ID.</param>
        /// <param name="status">The new status.</param>
        /// <param name="progress">The progress of the task.</param>
        /// <param name="metrics">The metrics collected during task execution.</param>
        public TaskStatusChangedJetHeartbeatData(Guid jobId, TaskAttemptId taskAttemptId, TaskAttemptStatus status, TaskProgress progress, TaskMetrics metrics)
        {
            if (taskAttemptId == null)
                throw new ArgumentNullException(nameof(taskAttemptId));

            JobId = jobId;
            TaskAttemptId = taskAttemptId;
            Status = status;
            Progress = progress;
            Metrics = metrics;
        }

        /// <summary>
        /// Gets the ID of the job containing the task.
        /// </summary>
        public Guid JobId { get; private set; }

        /// <summary>
        /// Gets the ID of the task whose status has changed.
        /// </summary>
        public TaskAttemptId TaskAttemptId { get; private set; }

        /// <summary>
        /// Gets the new status of the task.
        /// </summary>
        public TaskAttemptStatus Status { get; private set; }

        /// <summary>
        /// Gets the progress of the task.
        /// </summary>
        public TaskProgress Progress { get; private set; }

        /// <summary>
        /// Gets the metrics collected during task execution.
        /// </summary>
        /// <value>The metrics.</value>
        public TaskMetrics Metrics { get; private set; }
    }
}
