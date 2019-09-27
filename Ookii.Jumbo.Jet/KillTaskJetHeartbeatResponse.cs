// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Response sent by the job server if the task server must kill the specified task.
    /// </summary>
    /// <remarks>
    /// If the job server sends this reponse, it means it isn't interested in any future notifications about this task.
    /// </remarks>
    [Serializable]
    public sealed class KillTaskJetHeartbeatResponse : JetHeartbeatResponse
    {
        private readonly Guid _jobId;
        private readonly TaskAttemptId _taskAttemptId;

        /// <summary>
        /// Initializes a new instance of the <see cref="KillTaskJetHeartbeatResponse"/> class.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskAttemptId">The task attempt ID.</param>
        public KillTaskJetHeartbeatResponse(Guid jobId, TaskAttemptId taskAttemptId)
            : base(TaskServerHeartbeatCommand.KillTask)
        {
            if( taskAttemptId == null )
                throw new ArgumentNullException(nameof(taskAttemptId));

            _jobId = jobId;
            _taskAttemptId = taskAttemptId;
        }

        /// <summary>
        /// Gets the job ID.
        /// </summary>
        /// <value>The job ID.</value>
        public Guid JobId
        {
            get { return _jobId; }
        }

        /// <summary>
        /// Gets the task attempt ID.
        /// </summary>
        /// <value>The task attempt ID.</value>
        public TaskAttemptId TaskAttemptId
        {
            get { return _taskAttemptId; }
        }
    }
}
