// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Raw comparer for the <see cref="ValSortRecord"/> class.
    /// </summary>
    public class ValSortRecordRawComparer : IRawComparer<ValSortRecord>
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
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            int result = RawComparerHelper.CompareBytesWith7BitEncodedLength(buffer1, offset1, count1, buffer2, offset2, count2);
            if( result == 0 )
            {
                int length = Utf8String.GetLength(buffer1, offset1);
                long inputOffset1 = LittleEndianBitConverter.ToInt64(buffer1, offset1 + length);
                long inputOffset2 = LittleEndianBitConverter.ToInt64(buffer2, offset2 + length);
                result = inputOffset1.CompareTo(inputOffset2);
            }

            return result;
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(ValSortRecord x, ValSortRecord y)
        {
            return Comparer<ValSortRecord>.Default.Compare(x, y);
        }
    }
}
