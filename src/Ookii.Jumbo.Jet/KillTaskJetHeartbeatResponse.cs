// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Response sent by the job server if the task server must kill the specified task.
/// </summary>
/// <remarks>
/// If the job server sends this reponse, it means it isn't interested in any future notifications about this task.
/// </remarks>
[GeneratedWritable]
public sealed partial class KillTaskJetHeartbeatResponse : JetHeartbeatResponse
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KillTaskJetHeartbeatResponse"/> class.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    /// <param name="taskAttemptId">The task attempt ID.</param>
    public KillTaskJetHeartbeatResponse(Guid jobId, TaskAttemptId taskAttemptId)
        : base(TaskServerHeartbeatCommand.KillTask)
    {
        ArgumentNullException.ThrowIfNull(taskAttemptId);

        JobId = jobId;
        TaskAttemptId = taskAttemptId;
    }

    /// <summary>
    /// Gets the job ID.
    /// </summary>
    /// <value>The job ID.</value>
    public Guid JobId { get; private set; }

    /// <summary>
    /// Gets the task attempt ID.
    /// </summary>
    /// <value>The task attempt ID.</value>
    public TaskAttemptId TaskAttemptId { get; private set; }
}
