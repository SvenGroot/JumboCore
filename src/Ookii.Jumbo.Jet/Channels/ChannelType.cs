// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// The type of a communication channel between two tasks.
/// </summary>
public enum ChannelType
{
    /// <summary>
    /// The input task writes a file to disk, which the output task then downloads and reads from.
    /// </summary>
    File,
    /// <summary>
    /// The input task's output is directly pipelined to the output task.
    /// </summary>
    /// <remarks>
    /// Tasks connected by
    /// this channel type are treated as a single entity from the scheduler's point of view because they
    /// are executed in the same process.
    /// </remarks>
    Pipeline,
    /// <summary>
    /// The input task connects to the output task via TCP.
    /// </summary>
    /// <remarks>
    /// This channel has some limitations: all output tasks of the channel must run simultaneously,
    /// and the job will fail if a single task fails.
    /// </remarks>
    Tcp
}
