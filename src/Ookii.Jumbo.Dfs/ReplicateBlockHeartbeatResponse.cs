// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Provides data for a <see cref="DataServerHeartbeatCommand.ReplicateBlock"/> command.
/// </summary>
[GeneratedWritable]
public partial class ReplicateBlockHeartbeatResponse : HeartbeatResponse
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ReplicateBlockHeartbeatResponse" /> class.
    /// </summary>
    /// <param name="fileSystemId">The file system id.</param>
    /// <param name="blockAssignment">The assignment information for the block to replicate.</param>
    /// <exception cref="System.ArgumentNullException">blockAssignment</exception>
    public ReplicateBlockHeartbeatResponse(Guid fileSystemId, BlockAssignment blockAssignment)
        : base(fileSystemId, DataServerHeartbeatCommand.ReplicateBlock)
    {
        ArgumentNullException.ThrowIfNull(blockAssignment);

        BlockAssignment = blockAssignment;
    }

    /// <summary>
    /// Gets the new assignment information for the block to be replicated.
    /// </summary>
    /// <value>
    /// The <see cref="BlockAssignment"/> that contains the new assignment information for the block to be replicated.
    /// </value>
    public BlockAssignment BlockAssignment { get; private set; }
}
