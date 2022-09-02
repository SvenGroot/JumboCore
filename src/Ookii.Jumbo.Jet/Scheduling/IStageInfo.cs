// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Scheduling
{
    /// <summary>
    /// Provides information about a stage to a scheduler.
    /// </summary>
    public interface IStageInfo
    {
        /// <summary>
        /// Gets the stage ID.
        /// </summary>
        /// <value>
        /// The stage ID.
        /// </value>
        string StageId { get; }

        /// <summary>
        /// Gets the stage configuration.
        /// </summary>
        /// <value>
        /// The <see cref="StageConfiguration"/> for the stage.
        /// </value>
        StageConfiguration Configuration { get; }

        /// <summary>
        /// Gets the tasks in this stage.
        /// </summary>
        /// <value>
        /// A list of the tasks.
        /// </value>
        IEnumerable<ITaskInfo> Tasks { get; }

        /// <summary>
        /// Gets the number of unscheduled tasks in this stage.
        /// </summary>
        /// <value>
        /// The number of unscheduled tasks in this stage.
        /// </value>
        /// <remarks>
        ///   Unscheduled tasks are all tasks that are not currently assigned to a task server. This may include tasks that need to be re-executed due to a failure.
        /// </remarks>
        int UnscheduledTaskCount { get; }

        /// <summary>
        /// Gets a value indicating whether this stage is ready to be scheduled.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if all the scheduling dependencies for this stage are met; otherwise, <see langword="false" />.
        /// </value>
        bool IsReadyForScheduling { get; }
    }
}
