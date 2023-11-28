// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// The function the data server should perform for a client.
/// </summary>
public enum DataServerCommand
{
    /// <summary>
    /// The client wants to read a block from the data server.
    /// </summary>
    ReadBlock,
    /// <summary>
    /// The client wants to write a block to the data server.
    /// </summary>
    WriteBlock,
    /// <summary>
    /// The clients wants to read the contents of the data servers diagnostic log file.
    /// </summary>
    GetLogFileContents
}
