﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the data sent by a data server during a heartbeat when it informs the name server
    /// it has received a new block.
    /// </summary>
    [Serializable]
    public class NewBlockHeartbeatData : StatusHeartbeatData
    {
        /// <summary>
        /// Gets or sets the block ID.
        /// </summary>
        /// <value>
        /// A <see cref="Guid"/> that uniquely identifies the block.
        /// </value>
        /// 
        public Guid BlockId { get; set; }

        /// <summary>
        /// Gets or sets the size of the block.
        /// </summary>
        /// <value>
        /// The size of the block, in bytes.
        /// </value>
        public int Size { get; set; }
    }
}
