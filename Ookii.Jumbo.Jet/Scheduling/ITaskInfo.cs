// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Scheduling
{
    /// <summary>
    /// Provides information about a task to a scheduler.
    /// </summary>
    public interface ITaskInfo
    {
        /// <summary>
        /// Gets the task ID.
        /// </summary>
        /// <value>
        /// The task ID.
        /// </value>
        TaskId TaskId { get; }

        /// <summary>
        /// Gets the task ID as a <see cref="String"/>, including the job ID.
        /// </summary>
        /// <value>
        /// The full task id.
        /// </value>
        string FullTaskId { get; }

        /// <summary>
        /// Gets a value indicating whether this task is assigned to a server.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if this task is assigned to a server; otherwise, <see langword="false" />.
        /// </value>
        bool IsAssignedToServer { get; }

        /// <summary>
        /// Determines whether the specified server has experienced a failure on this task before.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <returns>
        ///   <see langword="true" /> if the specified server previously failed to execute this task; otherwise, <see langword="false" />.
        /// </returns>
        bool IsBadServer(ITaskServerJobInfo server);
    }
}
