﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A raw comparer for <see cref="Utf8String"/> records.
    /// </summary>
    /// <note>
    ///   Instances of the <see cref="Utf8String"/> class will not compare in proper lexicographical order if the string contains multi-byte characters.
    ///   All that is guaranteed is that the ordering is deterministic.
    /// </note>
    public sealed class Utf8StringRawComparer : IRawComparer<Utf8String>
    {
        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="buffer1">The buffer containing the first object.</param>
        /// <param name="offset1">The offset into <paramref name="buffer1"/> where the first object starts.</param>
        /// <param name="count1">The number of bytes in <paramref name="buffer1"/> used by the first object.</param>
        /// <param name="buffer2">The buffer containing the second object.</param>
        /// <param name="offset2">The offset into <paramref name="buffer2"/> where the second object starts.</param>
        /// <param name="count2">The number of bytes in <paramref name="buffer2"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        /// <remarks>
        /// <para>
        ///   The values of <paramref name="count1"/> and <paramref name="count2"/> may be larger than the size of the record.
        ///   The comparer should determine on its own the actual size of the record, in the same way the <see cref="IWritable"/>
        ///   or <see cref="ValueWriter{T}"/> for that record does, and use that for the comparison. You should however
        ///   never read more bytes from the buffer than the specified count.
        /// </para>
        /// </remarks>
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            return RawComparerHelper.CompareBytesWith7BitEncodedLength(buffer1, offset1, count1, buffer2, offset2, count2);
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(Utf8String x, Utf8String y)
        {
            return Comparer<Utf8String>.Default.Compare(x, y);
        }
    }
}
