// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Heartbeat response used when the job server wants the task server to clean up data related to a job.
/// </summary>
[GeneratedWritable]
public partial class CleanupJobJetHeartbeatResponse : JetHeartbeatResponse
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CleanupJobJetHeartbeatResponse"/> class.
    /// </summary>
    /// <param name="jobId">The job ID.</param>
    public CleanupJobJetHeartbeatResponse(Guid jobId)
        : base(TaskServerHeartbeatCommand.CleanupJob)
    {
        JobId = jobId;
    }

    /// <summary>
    /// Gets the job ID of the job whose data to clean up.
    /// </summary>
    public Guid JobId { get; private set; }
}
