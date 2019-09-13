// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the header sent by a client to the data server when reading a block.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolReadHeader : DataServerClientProtocolHeader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolReadHeader"/> class.
        /// </summary>
        public DataServerClientProtocolReadHeader()
            : base(DataServerCommand.ReadBlock)
        {
        }

        /// <summary>
        /// Gets or sets the offset into the block at which to start reading.
        /// </summary>
        /// <value>
        /// The offset into the block, in bytes, at which to start reading.
        /// </value>
        public int Offset { get; set; }

        /// <summary>
        /// Gets or sets the size of the data to read.
        /// </summary>
        /// <value>
        /// The size of the data to read, in bytes.
        /// </value>
        public int Size { get; set; }
    }
}
