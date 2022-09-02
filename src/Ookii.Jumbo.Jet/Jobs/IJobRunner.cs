// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Interface for classes that provide job running services.
    /// </summary>
    public interface IJobRunner
    {
        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        Guid RunJob();

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        void FinishJob(bool success);
    }
}
