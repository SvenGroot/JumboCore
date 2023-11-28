// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Indicates how to read or write a <see cref="Packet"/>.
/// </summary>
public enum PacketFormatOption
{
    /// <summary>
    /// Reads or writes all fields.
    /// </summary>
    Default,
    /// <summary>
    /// Reads or writes all fields except the sequence number.
    /// </summary>
    NoSequenceNumber,
    /// <summary>
    /// Reads or writes only the checksum and the data.
    /// </summary>
    ChecksumOnly
}
