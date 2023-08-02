// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Raw comparer for the <see cref="Pair{TKey,TValue}"/> class.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public class PairRawComparer<TKey, TValue> : IRawComparer<Pair<TKey, TValue>>, IDeserializingRawComparer
        where TKey : notnull, IComparable<TKey>
        where TValue : notnull
    {
        private readonly IRawComparer<TKey> _comparer;

        /// <summary>
        /// Initializes a new instance of the <see cref="PairRawComparer{TKey, TValue}"/> class.
        /// </summary>
        public PairRawComparer()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PairRawComparer{TKey, TValue}"/> class.
        /// </summary>
        /// <param name="comparer">The key comparer.</param>
        public PairRawComparer(IRawComparer<TKey>? comparer)
        {
            _comparer = comparer ?? RawComparer<TKey>.CreateComparer();
        }

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
            return _comparer.Compare(buffer1, offset1, count1, buffer2, offset2, count2);
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(Pair<TKey, TValue>? x, Pair<TKey, TValue>? y)
        {
            if (x == null)
            {
                if (y == null)
                    return 0;
                else
                    return -1;
            }
            else if (y == null)
                return 1;
            return _comparer.Compare(x.Key, y.Key);
        }

        /// <summary>
        /// Gets a value indicating whether the comparer uses deserialization.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if whether the comparer uses deserialization; otherwise, <see langword="false" />.
        /// </value>
        public bool UsesDeserialization
        {
            get
            {
                var keyComparer = _comparer as IDeserializingRawComparer;
                return keyComparer != null && keyComparer.UsesDeserialization;
            }
        }
    }
}
