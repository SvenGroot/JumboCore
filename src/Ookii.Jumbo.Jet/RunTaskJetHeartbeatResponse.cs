// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat response used when the job server has a task that the task server should execute.
    /// </summary>
    [Serializable]
    public class RunTaskJetHeartbeatResponse : JetHeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RunTaskJetHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="job">The job containing the task to run.</param>
        /// <param name="taskAttemptId">The ID of the task to run.</param>
        public RunTaskJetHeartbeatResponse(Job job, TaskAttemptId taskAttemptId)
            : base(TaskServerHeartbeatCommand.RunTask)
        {
            ArgumentNullException.ThrowIfNull(job);
            ArgumentNullException.ThrowIfNull(taskAttemptId);

            Job = job;
            TaskAttemptId = taskAttemptId;
        }

        /// <summary>
        /// Gets the job containing the task to run.
        /// </summary>
        public Job Job { get; private set; }
        /// <summary>
        /// Gets the ID of the task attempt the server should run.
        /// </summary>
        public TaskAttemptId TaskAttemptId { get; private set; }
    }
}
