// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides helper methods for implementing raw comparers.
    /// </summary>
    public static class RawComparerHelper
    {
        /// <summary>
        /// Compares the binary representation of two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="self">The comparer to use.</param>
        /// <param name="record1">The first record.</param>
        /// <param name="record2">The second record.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public static int Compare<T>(this IRawComparer<T> self, RawRecord record1, RawRecord record2)
        {
            if (self == null)
                throw new ArgumentNullException(nameof(self));
            if (record1 == null)
            {
                if (record2 == null)
                    return 0;
                else
                    return -1;
            }
            else if (record2 == null)
                return 1;

            return self.Compare(record1.Buffer, record1.Offset, record1.Count, record2.Buffer, record2.Offset, record2.Count);
        }

        /// <summary>
        /// Helper method to compare a range of bytes.
        /// </summary>
        /// <param name="buffer1">The buffer containing the first object.</param>
        /// <param name="offset1">The offset into <paramref name="buffer1"/> where the first object starts.</param>
        /// <param name="count1">The number of bytes in <paramref name="buffer1"/> used by the first object.</param>
        /// <param name="buffer2">The buffer containing the second object.</param>
        /// <param name="offset2">The offset into <paramref name="buffer2"/> where the second object starts.</param>
        /// <param name="count2">The number of bytes in <paramref name="buffer2"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public static unsafe int CompareBytes(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            fixed (byte* str1ptr = buffer1, str2ptr = buffer2)
            {
                var left = str1ptr + offset1;
                var end = left + Math.Min(count1, count2);
                var right = str2ptr + offset2;
                while (left < end)
                {
                    if (*left != *right)
                        return *left - *right;
                    ++left;
                    ++right;
                }
                return count1 - count2;
            }
        }

        /// <summary>
        /// Helper method to compare a range of bytes with a 7-bit encoded length before the range.
        /// </summary>
        /// <param name="buffer1">The buffer containing the first object.</param>
        /// <param name="offset1">The offset into <paramref name="buffer1"/> where the first object starts.</param>
        /// <param name="count1">The number of bytes in <paramref name="buffer1"/> used by the first object.</param>
        /// <param name="buffer2">The buffer containing the second object.</param>
        /// <param name="offset2">The offset into <paramref name="buffer2"/> where the second object starts.</param>
        /// <param name="count2">The number of bytes in <paramref name="buffer2"/> used by the second object.</param>
        /// <returns>A signed integer that indicates the relative values of the first and second object.</returns>
        public static unsafe int CompareBytesWith7BitEncodedLength(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            fixed (byte* str1ptr = buffer1, str2ptr = buffer2)
            {
                var left = str1ptr + offset1;
                var right = str2ptr + offset2;
                var length1 = Decode7BitEncodedInt32(ref left);
                var length2 = Decode7BitEncodedInt32(ref right);
                var end = left + Math.Min(length1, length2);
                while (left < end)
                {
                    if (*left != *right)
                        return *left - *right;
                    ++left;
                    ++right;
                }
                return count1 - count2;
            }
        }

        private static unsafe int Decode7BitEncodedInt32(ref byte* buffer)
        {
            byte currentByte;
            var result = 0;
            var bits = 0;
            do
            {
                if (bits == 35)
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = *buffer++;
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while ((currentByte & 0x80) != 0);
            return result;
        }

        internal static IRawComparer<T> GetComparer<T>()
        {
            var type = typeof(T);
            var attribute = (RawComparerAttribute)Attribute.GetCustomAttribute(type, typeof(RawComparerAttribute));
            if (attribute != null && !string.IsNullOrEmpty(attribute.RawComparerTypeName))
            {
                var comparerType = Type.GetType(attribute.RawComparerTypeName);
                if (comparerType.IsGenericTypeDefinition && type.IsGenericType)
                    comparerType = comparerType.MakeGenericType(type.GetGenericArguments());
                return (IRawComparer<T>)Activator.CreateInstance(comparerType);
            }

            return (IRawComparer<T>)DefaultRawComparer.GetComparer(type);
        }

    }
}
