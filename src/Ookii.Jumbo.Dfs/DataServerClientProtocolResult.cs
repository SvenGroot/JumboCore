// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// A status code sent by the data server when it received a packet.
/// </summary>
public enum DataServerClientProtocolResult
{
    /// <summary>
    /// The packet was successfully received and written to disk.
    /// </summary>
    Ok,
    /// <summary>
    /// An error occurred while receiving or processing the packet.
    /// </summary>
    Error,
    /// <summary>
    /// The requested block offset was larger than the block size.
    /// </summary>
    OutOfRange,
}
