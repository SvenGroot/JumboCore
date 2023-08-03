// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Raw comparer for the <see cref="GenSortRecord"/> class.
    /// </summary>
    public sealed class GenSortRecordRawComparer : IRawComparer<GenSortRecord>
    {
        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The buffer containing the first object.</param>
        /// <param name="xOffset">The offset into <paramref name="x"/> where the first object starts.</param>
        /// <param name="xCount">The number of bytes in <paramref name="x"/> used by the first object.</param>
        /// <param name="y">The buffer containing the second object.</param>
        /// <param name="yOffset">The offset into <paramref name="y"/> where the second object starts.</param>
        /// <param name="yCount">The number of bytes in <paramref name="y"/> used by the second object.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(byte[] x, int xOffset, int xCount, byte[] y, int yOffset, int yCount)
        {
            return RawComparerHelper.CompareBytes(x, xOffset, GenSortRecord.KeySize, y, yOffset, GenSortRecord.KeySize);
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(GenSortRecord? x, GenSortRecord? y)
        {
            return Comparer<GenSortRecord>.Default.Compare(x, y);
        }
    }
}
