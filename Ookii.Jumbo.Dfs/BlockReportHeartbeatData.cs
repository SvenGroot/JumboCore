// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent during a heartbeat when the data server is sending a block report.
    /// </summary>
    [Serializable]
    public class BlockReportHeartbeatData : StatusHeartbeatData
    {
        private readonly ReadOnlyCollection<Guid> _blocks;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockReportHeartbeatData"/> class.
        /// </summary>
        /// <param name="blocks">The list of blocks that this data server has.</param>
        public BlockReportHeartbeatData(IEnumerable<Guid> blocks)
        {
            if (blocks == null)
                throw new ArgumentNullException(nameof(blocks));
            _blocks = new List<Guid>(blocks).AsReadOnly();
        }

        /// <summary>
        /// Gets the the blocks that are stored on this data server.
        /// </summary>
        /// <value>
        /// A list of block IDs for the blocks stored on this data server.
        /// </value>
        public ReadOnlyCollection<Guid> Blocks
        {
            get { return _blocks; }
        }
    }
}
