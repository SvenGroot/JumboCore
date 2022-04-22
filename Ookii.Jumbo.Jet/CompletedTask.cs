// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides information about a task that has finished executing.
    /// </summary>
    [Serializable]
    public class CompletedTask
    {
        /// <summary>
        /// Gets or sets the job ID that this task is part of.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the task attempt ID that finished this task.
        /// </summary>
        public TaskAttemptId TaskAttemptId { get; set; }

        /// <summary>
        /// Gets the global task ID of the task.
        /// </summary>
        public string FullTaskId
        {
            get
            {
                return Job.CreateFullTaskId(JobId, TaskAttemptId);
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="ServerAddress"/> of the task server that ran the task.
        /// </summary>
        /// <remarks>
        /// When using the <see cref="Channels.FileInputChannel"/>, this is the server where the output data can be downloaded.
        /// </remarks>
        public ServerAddress TaskServer { get; set; }

        /// <summary>
        /// Gets or sets the port that the task server listens on for downloading file channel data.
        /// </summary>
        public int TaskServerFileServerPort { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return TaskAttemptId == null ? "" : TaskAttemptId.ToString();
        }
    }
}
