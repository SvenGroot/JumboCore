// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Index entry for a partition file. For Jumbo internal use only.
    /// </summary>
    [ValueWriter(typeof(PartitionFileIndexEntryValueWriter))]
    public struct PartitionFileIndexEntry : IEquatable<PartitionFileIndexEntry>
    {
        private readonly int _partition;
        private readonly long _offset;
        private readonly long _compressedSize;
        private readonly long _uncompressedSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionFileIndexEntry" /> struct.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="compressedSize">Size of the compressed data.</param>
        /// <param name="uncompressedSize">Size of the uncompressed data.</param>
        public PartitionFileIndexEntry(int partition, long offset, long compressedSize, long uncompressedSize)
        {
            _partition = partition;
            _offset = offset;
            _compressedSize = compressedSize;
            _uncompressedSize = uncompressedSize;
        }

        /// <summary>
        /// Gets or sets the partition.
        /// </summary>
        /// <value>The partition.</value>
        public int Partition
        {
            get { return _partition; }
        }


        /// <summary>
        /// Gets or sets the offset.
        /// </summary>
        /// <value>The offset.</value>
        public long Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// Gets the size of the compressed data.
        /// </summary>
        /// <value>
        /// The size of the compressed data.
        /// </value>
        public long CompressedSize
        {
            get { return _compressedSize; }
        }

        /// <summary>
        /// Gets the size of the compressed data.
        /// </summary>
        /// <value>
        /// The size of the compressed data.
        /// </value>
        public long UncompressedSize
        {
            get { return _uncompressedSize; }
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        ///   <see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
        /// </returns>
        public override bool Equals(object? obj)
        {
            var entry = obj as RecordIndexEntry?;
            if (entry == null)
                return false;
            return Equals(entry.Value);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns><see langword="true"/> if the current object is equal to the other parameter; otherwise, <see langword="false"/>.</returns>
        public bool Equals(PartitionFileIndexEntry other)
        {
            return _partition == other._partition && _offset == other.Offset && _uncompressedSize == other._uncompressedSize && _compressedSize == other._compressedSize;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _partition.GetHashCode() ^ _offset.GetHashCode() ^ _compressedSize.GetHashCode() ^ _uncompressedSize.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified instances have the same value.
        /// </summary>
        /// <param name="left">The first instance to compare.</param>
        /// <param name="right">The second instance to compare.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator ==(PartitionFileIndexEntry left, PartitionFileIndexEntry right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Determines whether two specified instances have different values.
        /// </summary>
        /// <param name="left">The first instance to compare.</param>
        /// <param name="right">The second instance to compare.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator !=(PartitionFileIndexEntry left, PartitionFileIndexEntry right)
        {
            return !left.Equals(right);
        }
    }
}
