// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// The interface used by clients to communicate with the job server.
/// </summary>
[RpcInterface]
public interface IJobServerClientProtocol
{
    /// <summary>
    /// Creates a new job and assigns a directory on the distributed file system where the job's files are meant
    /// to be stored.
    /// </summary>
    /// <returns>An instance of the <see cref="Job"/> class containing information about the job.</returns>
    Job CreateJob();

    /// <summary>
    /// Begins execution of a job.
    /// </summary>
    /// <param name="jobId">The ID of the job to run.</param>
    void RunJob(Guid jobId);

    /// <summary>
    /// Aborts execution of a job.
    /// </summary>
    /// <param name="jobId">The ID of the job to abort.</param>
    /// <returns>
    ///   <see langword="true"/> if the job was aborted; otherwise, <see langword="false"/>.
    /// </returns>
    /// <remarks>
    /// If the job was created but not started, calling this method will remove the job from the pending queue, and the
    /// method will return <see langword="true"/>.
    /// </remarks>
    bool AbortJob(Guid jobId);

    /// <summary>
    /// Gets the address of the task server that is running the specified task.
    /// </summary>
    /// <param name="jobId">The ID of the job containing the task.</param>
    /// <param name="taskId">The ID of the task.</param>
    /// <returns>The <see cref="ServerAddress"/> for the task server that is running the task.</returns>
    ServerAddress GetTaskServerForTask(Guid jobId, string taskId);

    /// <summary>
    /// Waits until any of the specified tasks complete.
    /// </summary>
    /// <param name="jobId">The ID of the job containing the tasks.</param>
    /// <param name="tasks">The IDs of the tasks to wait for.</param>
    /// <returns>A <see cref="CompletedTask"/> instance indicating which of the tasks completed.</returns>
    CompletedTask[] CheckTaskCompletion(Guid jobId, string[] tasks);

    /// <summary>
    /// Gets the current status for the specified job.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <returns>The status of the job, or <see langword="null"/> if the job doesn't exist.</returns>
    JobStatus GetJobStatus(Guid jobId);

    /// <summary>
    /// Gets the status for all currently running jobs.
    /// </summary>
    /// <returns>An array of status objects for each running job.</returns>
    JobStatus[] GetRunningJobs();

    /// <summary>
    /// Gets current metrics for the distributed execution engine.
    /// </summary>
    /// <returns>An object holding the metrics for the job server.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
    JetMetrics GetMetrics();

    /// <summary>
    /// Gets the contents of the diagnostic log file.
    /// </summary>
    /// <param name="kind">The kind of log file to return.</param>
    /// <param name="maxSize">The maximum size of the log data to return.</param>
    /// <returns>The contents of the diagnostic log file.</returns>
    string GetLogFileContents(LogFileKind kind, int maxSize);

    /// <summary>
    /// Gets a list of archived jobs.
    /// </summary>
    /// <returns>A list of archived jobs.</returns>
    ArchivedJob[] GetArchivedJobs();

    /// <summary>
    /// Gets the job status for an archived job.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <returns>The status for the archived job, or <see langword="null"/> if the job wasn't found.</returns>
    JobStatus GetArchivedJobStatus(Guid jobId);

    /// <summary>
    /// Gets the contents of a job configuration file.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <param name="archived">If set to <see langword="true" />, search the archived job directory instead of the active jobs.</param>
    /// <returns>
    /// The raw XML of the archived job's configuration, or <see langword="null" /> if the job wasn't found.
    /// </returns>
    string GetJobConfigurationFile(Guid jobId, bool archived);
}
