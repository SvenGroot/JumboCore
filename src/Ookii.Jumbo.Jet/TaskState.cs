// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Indicates the current state of a task.
/// </summary>
public enum TaskState
{
    /// <summary>
    /// The task has been loaded by the job server, but has not been assigned to a task server yet.
    /// </summary>
    Created,
    /// <summary>
    /// The task has been assigned to a task server, but has not been started yet.
    /// </summary>
    Scheduled,
    /// <summary>
    /// The task server has been told to start executing the task.
    /// </summary>
    Running,
    /// <summary>
    /// The task has finished executing successfully.
    /// </summary>
    Finished,
    /// <summary>
    /// The task has encountered an error in its previous attempt.
    /// </summary>
    Error,
    /// <summary>
    /// The task was aborted (usually because the job failed).
    /// </summary>
    Aborted
}
