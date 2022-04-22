// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the header a client sends to a data server when writing a block.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolWriteHeader : DataServerClientProtocolHeader
    {
        private readonly ReadOnlyCollection<ServerAddress> _dataServers;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolWriteHeader"/> class.
        /// </summary>
        /// <param name="dataServers">The list of data servers that this block should be written to.</param>
        public DataServerClientProtocolWriteHeader(IEnumerable<ServerAddress> dataServers)
            : base(DataServerCommand.WriteBlock)
        {
            if (dataServers == null)
                throw new ArgumentNullException(nameof(dataServers));
            _dataServers = new List<ServerAddress>(dataServers).AsReadOnly();
        }

        /// <summary>
        /// Gets or sets the data servers that this block should be written to.
        /// </summary>
        /// <value>
        /// A list of <see cref="ServerAddress"/> objects for the data servers that this block should be written to.
        /// </value>
        /// <remarks>
        /// The first server in the list should be the data server this header is sent to. The server
        /// will forward the block to the next server in the list.
        /// </remarks>
        public ReadOnlyCollection<ServerAddress> DataServers
        {
            get { return _dataServers; }
        }
    }
}
