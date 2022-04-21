// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Value writer for the <see cref="PartitionFileIndexEntry"/> structure.
    /// </summary>
    public sealed class PartitionFileIndexEntryValueWriter : IValueWriter<PartitionFileIndexEntry>
    {
        /// <summary>
        /// Writes the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="writer">The writer.</param>
        public void Write(PartitionFileIndexEntry value, BinaryWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            writer.Write(value.Partition);
            writer.Write(value.Offset);
            writer.Write(value.CompressedSize);
            writer.Write(value.UncompressedSize);
        }

        /// <summary>
        /// Reads a value from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>The <see cref="PartitionFileIndexEntry"/> read from the reader.</returns>
        public PartitionFileIndexEntry Read(BinaryReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));
            return new PartitionFileIndexEntry(reader.ReadInt32(), reader.ReadInt64(), reader.ReadInt64(), reader.ReadInt64());
        }
    }
}
