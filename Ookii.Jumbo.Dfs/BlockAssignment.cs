// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides information about a block of a file.
    /// </summary>
    [Serializable]
    public class BlockAssignment
    {
        private readonly ReadOnlyCollection<ServerAddress> _dataServers;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockAssignment"/> class.
        /// </summary>
        /// <param name="blockId">The ID of the block.</param>
        /// <param name="dataServers">The list of data servers that have this block.</param>
        public BlockAssignment(Guid blockId, IEnumerable<ServerAddress> dataServers)
        {
            if( dataServers == null )
                throw new ArgumentNullException("dataServers");

            BlockId = blockId;
            _dataServers = new List<ServerAddress>(dataServers).AsReadOnly();
        }

        /// <summary>
        /// Gets the unique identifier of this block.
        /// </summary>
        /// <value>
        /// A <see cref="Guid"/> that uniquely identifies this block.
        /// </value>
        public Guid BlockId { get; private set; }

        /// <summary>
        /// Gets the data servers that have a replica of this block.
        /// </summary>
        /// <value>
        /// A collection of <see cref="ServerAddress"/> objects for the data servers that have this block.
        /// </value>
        public ReadOnlyCollection<ServerAddress> DataServers
        {
            get { return _dataServers; }
        }
    }
}
