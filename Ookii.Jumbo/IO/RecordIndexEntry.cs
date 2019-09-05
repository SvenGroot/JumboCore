// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Represents an index entry indicating the position of a record in an array of bytes.
    /// </summary>
    public struct RecordIndexEntry : IEquatable<RecordIndexEntry>
    {
        private readonly int _offset;
        private readonly int _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordIndexEntry"/> struct.
        /// </summary>
        /// <param name="offset">The offset into the byte array.</param>
        /// <param name="count">The number of bytes for the record.</param>
        public RecordIndexEntry(int offset, int count)
        {
            _offset = offset;
            _count = count;
        }

        /// <summary>
        /// Gets the offset into the byte array.
        /// </summary>
        /// <value>
        /// The offset into the byte array.
        /// </value>
        public int Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// Gets the number of bytes for the record.
        /// </summary>
        public int Count
        {
            get { return _count; }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "Offset: {0}; Count: {1}", Offset, Count);
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        ///   <see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
        /// </returns>
        public override bool Equals(object obj)
        {
            RecordIndexEntry? entry = obj as RecordIndexEntry?;
            if( entry == null )
                return false;
            return Equals(entry.Value);
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <param name="other">An object to compare with this object.</param>
        /// <returns><see langword="true"/> if the current object is equal to the other parameter; otherwise, <see langword="false"/>.</returns>
        public bool Equals(RecordIndexEntry other)
        {
            return _offset == other.Offset && _count == other._count;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _offset.GetHashCode() ^ _count.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified instances have the same value.
        /// </summary>
        /// <param name="left">The first instance to compare.</param>
        /// <param name="right">The second instance to compare.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator ==(RecordIndexEntry left, RecordIndexEntry right)
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
        public static bool operator !=(RecordIndexEntry left, RecordIndexEntry right)
        {
            return !left.Equals(right);
        }
    }
}
