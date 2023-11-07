// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides information about a block of a file.
    /// </summary>
    [ValueWriter(typeof(Writer))]
    public class BlockAssignment
    {
        #region Nested types

        public class Writer : IValueWriter<BlockAssignment>
        {
            public BlockAssignment Read(BinaryReader reader)
                => new(reader ?? throw new ArgumentNullException(nameof(reader)));

            public void Write(BlockAssignment value, BinaryWriter writer)
            {
                ArgumentNullException.ThrowIfNull(nameof(value));
                ArgumentNullException.ThrowIfNull(nameof(writer));
                ValueWriter.WriteValue(value.BlockId, writer);
                ValueWriter.WriteValue(value.DataServers, writer);
            }
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockAssignment"/> class.
        /// </summary>
        /// <param name="blockId">The ID of the block.</param>
        /// <param name="dataServers">The list of data servers that have this block.</param>
        public BlockAssignment(Guid blockId, IEnumerable<ServerAddress> dataServers)
        {
            ArgumentNullException.ThrowIfNull(dataServers);

            BlockId = blockId;
            DataServers = dataServers.ToImmutableArray();
        }

        private BlockAssignment(BinaryReader reader)
        {
            BlockId = ValueWriter<Guid>.ReadValue(reader);
            DataServers = ValueWriter<ImmutableArray<ServerAddress>>.ReadValue(reader);
        }

        /// <summary>
        /// Gets the unique identifier of this block.
        /// </summary>
        /// <value>
        /// A <see cref="Guid"/> that uniquely identifies this block.
        /// </value>
        public Guid BlockId { get; }

        /// <summary>
        /// Gets the data servers that have a replica of this block.
        /// </summary>
        /// <value>
        /// A collection of <see cref="ServerAddress"/> objects for the data servers that have this block.
        /// </value>
        public ImmutableArray<ServerAddress> DataServers { get; }
    }
}
