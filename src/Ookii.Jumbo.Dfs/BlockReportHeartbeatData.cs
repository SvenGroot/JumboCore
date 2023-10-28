// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Ookii.Jumbo.IO;
using System.IO;
using System.Linq;
using System.Collections.Immutable;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent during a heartbeat when the data server is sending a block report.
    /// </summary>
    [GeneratedWritable]
    public partial class BlockReportHeartbeatData : StatusHeartbeatData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BlockReportHeartbeatData"/> class.
        /// </summary>
        /// <param name="blocks">The list of blocks that this data server has.</param>
        public BlockReportHeartbeatData(Guid[] blocks)
        {
            ArgumentNullException.ThrowIfNull(blocks);
            Blocks = blocks;
        }

        /// <summary>
        /// Gets the the blocks that are stored on this data server.
        /// </summary>
        /// <value>
        /// A list of block IDs for the blocks stored on this data server.
        /// </value>
        [WritableNotNull]
        public Guid[] Blocks { get; private set; }
    }
}
