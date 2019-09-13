// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Interface used by the TaskHost to communicate with its task server.
    /// </summary>
    public interface ITaskServerUmbilicalProtocol
    {
        /// <summary>
        /// Reports successful task completion to the task server.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task.</param>
        /// <param name="taskAttemptId">The task ID.</param>
        /// <param name="metrics">Metrics collected during the task execution.</param>
        void ReportCompletion(Guid jobId, TaskAttemptId taskAttemptId, TaskMetrics metrics);

        /// <summary>
        /// Reports progression of a task.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <param name="progress">The progress data.</param>
        void ReportProgress(Guid jobId, TaskAttemptId taskAttemptId, TaskProgress progress);

        /// <summary>
        /// Reports that the task has encountered an unrecoverable error.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <param name="failureReason">The failure reason.</param>
        void ReportError(Guid jobId, TaskAttemptId taskAttemptId, string failureReason);

        /// <summary>
        /// Registers the port number that the task host is listening on for TCP channel connections.
        /// </summary>
        /// <param name="jobId">The job ID of the job containing the task.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <param name="port">The port number.</param>
        void RegisterTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId, int port);

        /// <summary>
        /// Requests the task server to download a file from the DFS to make it available to all tasks.
        /// </summary>
        /// <param name="jobId">The ID of the job whose tasks need the file.</param>
        /// <param name="dfsPath">The path of the file to download.</param>
        /// <returns>The local path where the file was downloaded to.</returns>
        /// <remarks>
        /// <para>
        ///   You can use this method to download additional files from the DFS that need to be available to more than one task of a job
        ///   but weren't included in the job data when the job was created.
        /// </para>
        /// <para>
        ///   The task server will download the file only once; subsequent calls to this method (for the same job) will return the local
        ///   path of the previously downloaded file. This prevents the need for all tasks to download the same data from the DFS.
        /// </para>
        /// </remarks>
        string DownloadDfsFile(Guid jobId, string dfsPath);
    }
}
