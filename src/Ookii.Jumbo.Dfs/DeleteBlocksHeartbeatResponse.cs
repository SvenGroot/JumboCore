// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Provides data for the data server about which blocks it should delete.
/// </summary>
[GeneratedWritable]
public partial class DeleteBlocksHeartbeatResponse : HeartbeatResponse
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DeleteBlocksHeartbeatResponse" /> class.
    /// </summary>
    /// <param name="fileSystemId">The file system id.</param>
    /// <param name="blocks">A list of the identifiers of the blocks to delete.</param>
    public DeleteBlocksHeartbeatResponse(Guid fileSystemId, IEnumerable<Guid> blocks)
        : base(fileSystemId, DataServerHeartbeatCommand.DeleteBlocks)
    {
        ArgumentNullException.ThrowIfNull(blocks);
        Blocks = new List<Guid>(blocks);
    }

    /// <summary>
    /// Gets a list with the identifiers of the blocks to delete.
    /// </summary>
    /// <value>
    /// A list of <see cref="Guid"/> values that identity the blocks to delete.
    /// </value>
    public List<Guid> Blocks { get; private set; }
}
