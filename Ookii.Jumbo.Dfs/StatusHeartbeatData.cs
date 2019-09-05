// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides some general status data about the data server.
    /// </summary>
    [Serializable]
    public class StatusHeartbeatData : HeartbeatData
    {
        /// <summary>
        /// Gets or sets the total amount of disk space used by the blocks on this server.
        /// </summary>
        /// <value>
        /// The total amount of disk space used by the blocks on this server, in bytes.
        /// </value>
        public long DiskSpaceUsed { get; set; }

        /// <summary>
        /// Gets or sets the amount of free space on the disk holding the blocks.
        /// </summary>
        /// <value>
        /// The amount of free space on the disk holding the blocks, in bytes.
        /// </value>
        public long DiskSpaceFree { get; set; }

        /// <summary>
        /// Gets or sets the total size of the disk(s) holding the servers blocks.
        /// </summary>
        /// <value>
        /// The total size of the disk(s) holding the servers blocks, in bytes.
        /// </value>
        public long DiskSpaceTotal { get; set; }
    }
}
