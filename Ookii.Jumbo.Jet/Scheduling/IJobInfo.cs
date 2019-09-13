// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Scheduling
{
    /// <summary>
    /// Provides information about a job to a scheduler.
    /// </summary>
    public interface IJobInfo
    {
        /// <summary>
        /// The the ID of the job.
        /// </summary>
        /// <value>
        /// The job ID.
        /// </value>
        Guid JobId { get; }

        /// <summary>
        /// Gets the display name of the job.
        /// </summary>
        /// <value>
        /// The display name of the job.
        /// </value>
        string JobName { get; }

        /// <summary>
        /// Gets the configuration of the job.
        /// </summary>
        /// <value>
        /// The <see cref="JobConfiguration"/> for the job.
        /// </value>
        JobConfiguration Configuration { get; }

        /// <summary>
        /// Gets the number of unscheduled tasks.
        /// </summary>
        /// <value>
        /// The number of unscheduled tasks.
        /// </value>
        /// <remarks>
        ///   Unscheduled tasks are all tasks that are not currently assigned to a task server. This may include tasks that need to be re-executed due to a failure.
        /// </remarks>
        int UnscheduledTaskCount { get; }

        /// <summary>
        /// Gets the number of tasks that have successfully finished.
        /// </summary>
        /// <value>
        /// The number of tasks that have successfully finished.
        /// </value>
        int FinishedTaskCount { get; }

        /// <summary>
        /// Gets the number of task attempts that encountered an error.
        /// </summary>
        /// <value>
        /// The number of task attempts that encountered an error.
        /// </value>
        int ErrorCount { get; }

        /// <summary>
        /// Gets the stages of this job, in the order in which they should be scheduled.
        /// </summary>
        /// <value>
        /// A list of the job's stages.
        /// </value>
        /// <remarks>
        /// This list is ordered using the stage order returned by <see cref="JobConfiguration.GetDependencyOrderedStages"/>
        /// </remarks>
        IEnumerable<IStageInfo> Stages { get; }

        /// <summary>
        /// Gets job-specific information about the task servers.
        /// </summary>
        /// <value>
        /// A list of the task servers.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This list contains all task servers known to the job server when this scheduling pass started. These objects contain job-specific information
        ///   related to each server, so should only be used when scheduling tasks for this job.
        /// </para>
        /// </remarks>
        IEnumerable<ITaskServerJobInfo> TaskServers { get; }
    }
}
