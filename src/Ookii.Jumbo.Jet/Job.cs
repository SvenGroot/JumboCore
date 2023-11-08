// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Represents a job.
/// </summary>
[GeneratedValueWriter]
public partial class Job
{
    /// <summary>
    /// The name of the job configuration XML file.
    /// </summary>
    public const string JobConfigFileName = "job.xml";

    /// <summary>
    /// Initializes a new instance of the <see cref="Job"/> class with the specified ID and path.
    /// </summary>
    /// <param name="jobId">The unique identifier of this job.</param>
    /// <param name="path">The path on the distributed file system where files related to the job are stored.</param>
    public Job(Guid jobId, string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        JobId = jobId;
        Path = path;
    }

    /// <summary>
    /// Gets the unique identifier of this job.
    /// </summary>
    public Guid JobId { get; }

    /// <summary>
    /// Gets the path on the distributed file system where files related to the job are stored.
    /// </summary>
    public string Path { get; }

    /// <summary>
    /// Gets the path, including file name, of the job configuration file.
    /// </summary>
    /// <param name="client">The <see cref="FileSystemClient"/> to use to combine paths.</param>
    /// <returns>The full path of the job configuration file.</returns>
    public string GetJobConfigurationFilePath(FileSystemClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        return client.Path.Combine(Path, JobConfigFileName);
    }

    /// <summary>
    /// Creates the full task id for the specified task.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <param name="taskAttemptId">The task attempt id.</param>
    /// <returns>
    /// The full task ID of the form "{jobID}_taskID_attempt".
    /// </returns>
    public static string CreateFullTaskId(Guid jobId, TaskAttemptId taskAttemptId)
    {
        return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{{{0}}}_{1}", jobId, taskAttemptId);
    }

    /// <summary>
    /// Creates the full task id for the specified task.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <param name="taskId">The task ID.</param>
    /// <returns>The full task ID of the form "{jobID}_taskID".</returns>
    public static string CreateFullTaskId(Guid jobId, TaskId taskId)
    {
        return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{{{0}}}_{1}", jobId, taskId);
    }

}
