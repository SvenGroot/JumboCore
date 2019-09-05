// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// The protocol used when task servers communicate with each other or when the job server communicates
    /// with a task server other than its own.
    /// </summary>
    public interface ITaskServerClientProtocol
    {
        /// <summary>
        /// Gets the port on which the TCP server for the file channel listens.
        /// </summary>
        int FileServerPort { get; }

        /// <summary>
        /// Gets the current status of a task.
        /// </summary>
        /// <param name="jobId">The job id.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <returns>The status of the task.</returns>
        TaskAttemptStatus GetTaskStatus(Guid jobId, TaskAttemptId taskAttemptId);

        /// <summary>
        /// Gets the local directory where output files for a particular job are stored if that task uses a file output channel.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <returns>The output directory of the task.</returns>
        string GetOutputFileDirectory(Guid jobId);

        /// <summary>
        /// Gets the contents of the diagnostic log file.
        /// </summary>
        /// <param name="kind">The kind of log file to return.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>The contents of the diagnostic log file.</returns>
        string GetLogFileContents(LogFileKind kind, int maxSize);

        /// <summary>
        /// Gets the contents of the diagnostic log file for the specified task.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        /// <returns>
        /// The contents of the diagnostic log file, or <see langword="null"/> if it doesn't exist.
        /// </returns>
        string GetTaskLogFileContents(Guid jobId, TaskAttemptId taskAttemptId, int maxSize);

        /// <summary>
        /// Gets the contents of the diagnostic log files for all tasks of the specified job that this server has run,
        /// compressed into a zip file.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <returns>A byte array containing the zip file data.</returns>
        byte[] GetCompressedTaskLogFiles(Guid jobId);

        /// <summary>
        /// Gets the profile output for the specified task.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <returns>
        /// The profile output, or <see langword="null"/> if it doesn't exist.
        /// </returns>
        string GetTaskProfileOutput(Guid jobId, TaskAttemptId taskAttemptId);

        /// <summary>
        /// Gets the TCP server port for the specified task.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="taskAttemptId">The task attempt id.</param>
        /// <returns>
        /// The port number, or 0 if the task is unknown or hasn't registered a port number yet.
        /// </returns>
        int GetTcpChannelPort(Guid jobId, TaskAttemptId taskAttemptId);
    }
}
