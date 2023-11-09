// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Abstract base class for the header sent by a client when communicating with a data server.
/// </summary>
[ValueWriter(typeof(PolymorphicValueWriter<DataServerClientProtocolHeader>))]
[WritableDerivedType(typeof(DataServerClientProtocolWriteHeader))]
[WritableDerivedType(typeof(DataServerClientProtocolReadHeader))]
[WritableDerivedType(typeof(DataServerClientProtocolGetLogFileContentsHeader))]
[GeneratedWritable(Virtual = true)]
public abstract partial class DataServerClientProtocolHeader : IWritable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DataServerClientProtocolHeader"/> class with the specified command.
    /// </summary>
    /// <param name="command">The command to send to the server.</param>
    protected DataServerClientProtocolHeader(DataServerCommand command)
    {
        Command = command;
    }

    /// <summary>
    /// Gets or sets the command issued to the data server.
    /// </summary>
    /// <value>
    /// One of the <see cref="DataServerCommand"/> values indicating which command is issued to the data server.
    /// </value>
    public DataServerCommand Command { get; private set; }

    /// <summary>
    /// Gets or sets the block ID to be read or written.
    /// </summary>
    /// <value>
    /// The ID of the block to be read or written.
    /// </value>
    public Guid BlockId { get; set; }
}
