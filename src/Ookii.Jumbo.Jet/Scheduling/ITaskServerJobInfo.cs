// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet.Scheduling;

/// <summary>
/// Provides a job-specific view for a task server to a scheduler.
/// </summary>
public interface ITaskServerJobInfo
{
    /// <summary>
    /// Gets the address of the task server..
    /// </summary>
    /// <value>
    /// The <see cref="ServerAddress"/> for the task server.
    /// </value>
    ServerAddress Address { get; }

    /// <summary>
    /// Gets a value indicating whether this task server has reported status recently.
    /// </summary>
    /// <value>
    /// <see langword="true" /> if this task server has recently reported status to the job server; otherwise, <see langword="false" />.
    /// </value>
    bool IsActive { get; }

    /// <summary>
    /// Gets the number of available task slots.
    /// </summary>
    /// <value>
    /// The number of available task slots.
    /// </value>
    /// <remarks>
    /// The number of available slots is the total number of slots minus the number of tasks currently assigned to this server.
    /// </remarks>
    int AvailableTaskSlots { get; }

    /// <summary>
    /// Gets the number of tasks in the specified stage whose input data is local on this server.
    /// </summary>
    /// <param name="stage">The stage.</param>
    /// <returns>The number of tasks in the specified stage whose input data is local on this server.</returns>
    int GetLocalTaskCount(IStageInfo stage);

    /// <summary>
    /// Finds the a task using data input to schedule on this server.
    /// </summary>
    /// <param name="stage">The stage containing the tasks to schedule.</param>
    /// <param name="distance">The distance of the input data: 0 for local data, 1 for rack-local data, and 2 for non-local data.</param>
    /// <returns>The <see cref="ITaskInfo"/> for the task to schedule, or <see langword="null" /> if there is no task
    /// that can be scheduled.</returns>
    ITaskInfo FindDataInputTaskToSchedule(IStageInfo stage, int distance);

    /// <summary>
    /// Assigns the specified task to this server.
    /// </summary>
    /// <param name="task">The task.</param>
    /// <param name="dataDistance">The distance of the input data, if this task is a task with data input.</param>
    void AssignTask(ITaskInfo task, int? dataDistance = null);
}
