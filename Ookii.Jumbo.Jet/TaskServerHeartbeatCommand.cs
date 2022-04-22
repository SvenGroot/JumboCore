// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Commands that the job server can send to a task server in response to a hearbeat.
    /// </summary>
    public enum TaskServerHeartbeatCommand
    {
        /// <summary>
        /// The job server doesn't have a command for the task server.
        /// </summary>
        None,
        /// <summary>
        /// The task server should send a <see cref="InitialStatusJetHeartbeatData"/> in the next heartbeat.
        /// </summary>
        ReportStatus,
        /// <summary>
        /// The task server should execute the specified task.
        /// </summary>
        RunTask,
        /// <summary>
        /// Cleans up all data related to the tasks of the specified job.
        /// </summary>
        CleanupJob,
        /// <summary>
        /// The task server should kill the specified task attempt.
        /// </summary>
        KillTask
    }
}
