// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides context for a specific task attempt.
    /// </summary>
    public class TaskContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TaskContext"/> class.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="jobConfiguration">The configuration for the job.</param>
        /// <param name="taskAttemptId">The task attempt ID.</param>
        /// <param name="stageConfiguration">The configuration for the stage that the task belongs to.</param>
        /// <param name="localJobDirectory">The local directory where files related to the job are stored.</param>
        /// <param name="dfsJobDirectory">The DFS directory where files related to the job are stored.</param>
        public TaskContext(Guid jobId, JobConfiguration jobConfiguration, TaskAttemptId taskAttemptId, StageConfiguration stageConfiguration, string localJobDirectory, string dfsJobDirectory)
        {
            ArgumentNullException.ThrowIfNull(jobConfiguration);
            ArgumentNullException.ThrowIfNull(stageConfiguration);
            ArgumentNullException.ThrowIfNull(localJobDirectory);
            ArgumentNullException.ThrowIfNull(dfsJobDirectory);

            JobId = jobId;
            JobConfiguration = jobConfiguration;
            StageConfiguration = stageConfiguration;
            LocalJobDirectory = localJobDirectory;
            DfsJobDirectory = dfsJobDirectory;
            TaskAttemptId = taskAttemptId;
        }

        /// <summary>
        /// Gets the job ID.
        /// </summary>
        public Guid JobId { get; private set; }

        /// <summary>
        /// Gets the task ID.
        /// </summary>
        public TaskId TaskId
        {
            get { return TaskAttemptId.TaskId; }
        }

        /// <summary>
        /// Gets the configuration for the job.
        /// </summary>
        public JobConfiguration JobConfiguration { get; private set; }

        /// <summary>
        /// Gets the configuration for the stage that the task belong to.
        /// </summary>
        public StageConfiguration StageConfiguration { get; private set; }

        /// <summary>
        /// Gets the local directory where files related to the job are stored.
        /// </summary>
        public string LocalJobDirectory { get; private set; }

        /// <summary>
        /// Gets the directory on the DFS where files related to the job are stored.
        /// </summary>
        public string DfsJobDirectory { get; private set; }

        /// <summary>
        /// Gets the attempt number of this task attempt.
        /// </summary>
        public int Attempt
        {
            get { return TaskAttemptId.Attempt; }
        }

        /// <summary>
        /// Gets the task input for this task.
        /// </summary>
        /// <value>
        /// The task input, or <see langword="null"/> if this task doesn't have a task input.
        /// </value>
        public ITaskInput? TaskInput
        {
            get { return TaskExecution == null ? null : TaskExecution.TaskInput; }
        }


        /// <summary>
        /// Gets or sets the status message for the current task attempt.
        /// </summary>
        /// <remarks>
        /// Set this status message from task classes. This status message will be sent to the task server as part of a progress update.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1065:Do not raise exceptions in unexpected locations", Justification = "False positive.")]
        public string? StatusMessage
        {
            get
            {
                if (TaskExecution == null)
                    throw new InvalidOperationException("No task execution utility available.");
                return TaskExecution.TaskStatusMessage;
            }
            set
            {
                if (TaskExecution == null)
                    throw new InvalidOperationException("No task execution utility available.");
                TaskExecution.TaskStatusMessage = value;
            }
        }

        /// <summary>
        /// Gets the task attempt ID for this task attempt.
        /// </summary>
        public TaskAttemptId TaskAttemptId { get; set; }

        /// <summary>
        /// Forces a progress report to be sent on the next progress interval.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Call this method periodically if your task is executing a long-running operation that doesn't cause the task's progress to be changed (no input data is read).
        ///   This will ensure the job server doesn't think the task is hung.
        /// </para>
        /// <para>
        ///   Calling this method while your task is stuck in an infinite loop will cause the job to hang indefinitely.
        /// </para>
        /// <para>
        ///   If it's possible for your task to calculate progress for the long-running operation, consider implementing <see cref="IHasAdditionalProgress"/>
        ///   instead of calling this method.
        /// </para>
        /// </remarks>
        public void ReportProgress()
        {
            if (TaskExecution != null)
                TaskExecution.ReportProgress();
        }

        /// <summary>
        /// Requests the task server to download a file from the DFS to make it available to all tasks.
        /// </summary>
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
        public string DownloadDfsFile(string dfsPath)
        {
            ArgumentNullException.ThrowIfNull(dfsPath);

            if (TaskExecution == null)
                throw new InvalidOperationException("There's no TaskExecutionUtility associated with this instance.");

            return TaskExecution.Umbilical.DownloadDfsFile(JobId, dfsPath);
        }

        /// <summary>
        /// Gets a setting's string value with the specified default value, checking first in the stage settings and then in the job settings.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in either the stage or job settings.</returns>
        public string? GetSetting(string key, string? defaultValue)
        {
            return SettingsDictionary.GetJobOrStageSetting(JobConfiguration, StageConfiguration, key, defaultValue);
        }

        /// <summary>
        /// Gets a setting with the specified type and default value, checking first in the stage settings and then in the job settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
        /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in either the stage or job settings.</returns>
        public T? GetSetting<T>(string key, T? defaultValue)
        {
            return SettingsDictionary.GetJobOrStageSetting(JobConfiguration, StageConfiguration, key, defaultValue);
        }

        internal TaskExecutionUtility? TaskExecution { get; set; }
    }
}
