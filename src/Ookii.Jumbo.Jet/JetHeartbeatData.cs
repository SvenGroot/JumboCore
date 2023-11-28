// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Base class for data sent by the task servers to the job server.
/// </summary>
[ValueWriter(typeof(PolymorphicValueWriter<JetHeartbeatData>))]
[WritableDerivedType(typeof(InitialStatusJetHeartbeatData))]
[WritableDerivedType(typeof(TaskStatusChangedJetHeartbeatData))]
public abstract class JetHeartbeatData
{
}
