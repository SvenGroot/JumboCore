// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Heartbeat data informing the server of the status 
/// </summary>
[GeneratedWritable]
public partial class InitialStatusJetHeartbeatData : JetHeartbeatData
{
    /// <summary>
    /// Gets or sets the maximum number of tasks that this task server will accept.
    /// </summary>
    public int TaskSlots { get; set; }

    /// <summary>
    /// Gets or sets the port on which the task server accepts connections to download files for the
    /// file input channel.
    /// </summary>
    public int FileServerPort { get; set; }
}
