﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides data for the data server about which blocks it should delete.
    /// </summary>
    [Serializable]
    public class DeleteBlocksHeartbeatResponse : HeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteBlocksHeartbeatResponse" /> class.
        /// </summary>
        /// <param name="fileSystemId">The file system id.</param>
        /// <param name="blocks">A list of the identifiers of the blocks to delete.</param>
        public DeleteBlocksHeartbeatResponse(Guid fileSystemId, IEnumerable<Guid> blocks)
            : base(fileSystemId, DataServerHeartbeatCommand.DeleteBlocks)
        {
            if( blocks == null )
                throw new ArgumentNullException("blocks");
            Blocks = new List<Guid>(blocks);
        }

        /// <summary>
        /// Gets a list with the identifiers of the blocks to delete.
        /// </summary>
        /// <value>
        /// A list of <see cref="Guid"/> values that identiy the blocks to delete.
        /// </value>
        public IList<Guid> Blocks { get; private set; }
    }
}
