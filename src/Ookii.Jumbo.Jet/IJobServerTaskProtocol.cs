// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Protocol used by tasks to communicate with the job server. For Jumbo internal use only.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This interface is used by the <see cref="TaskExecutionUtility"/> class. It shouldn't be used elsewhere.
    /// </para>
    /// </remarks>
    public interface IJobServerTaskProtocol
    {
        /// <summary>
        /// Gets the partitions that are currently assigned to a task.
        /// </summary>
        /// <param name="jobId">The ID of the job containing the task.</param>
        /// <param name="taskId">The ID of the task.</param>
        /// <returns>A list of partition numbers that the task should process.</returns>
        int[] GetPartitionsForTask(Guid jobId, TaskId taskId);

        /// <summary>
        /// Notifies the job server that a task is about to start processing the specified partition.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskId">The task ID.</param>
        /// <param name="partitionNumber">The partition number.</param>
        /// <returns><see langword="true"/> if the task should start processing the partition; <see langword="false"/> if it has been reassigned to another task.</returns>
        /// <remarks>
        /// <para>
        ///   This method needn't be called for the first partition returned by <see cref="GetPartitionsForTask"/> or <see cref="GetAdditionalPartitions"/>; that partition is implicitly
        ///   considered to immediately start processing.
        /// </para>
        /// </remarks>
        bool NotifyStartPartitionProcessing(Guid jobId, TaskId taskId, int partitionNumber);

        /// <summary>
        /// Gets additional partitions for a task that has finished processing all its current partitions.
        /// </summary>
        /// <param name="jobId">The job id.</param>
        /// <param name="taskId">The task id.</param>
        /// <returns>A list of additional partition numbers to process, or <see langword="null"/> if the task should finish.</returns>
        int[] GetAdditionalPartitions(Guid jobId, TaskId taskId);
    }
}
