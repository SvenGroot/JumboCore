// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Comparer that inverts the results of the default raw comparer.
    /// </summary>
    /// <typeparam name="T">The type to compare</typeparam>
    /// <remarks>
    /// <para>
    ///   This comparer can be used e.g. to sort in descending order.
    /// </para>
    /// </remarks>
    public class InvertedRawComparer<T> : IRawComparer<T>
        where T : notnull
    {
        private readonly IRawComparer<T> _comparer = RawComparer<T>.CreateComparer();

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
            return _comparer.Compare(buffer2, offset2, count2, buffer1, offset1, count1);
        }

        /// <summary>
        /// Compares two objects and returns a value indicating whether one is less than, equal to, or greater than the other.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// A signed integer that indicates the relative values of the first and second object.
        /// </returns>
        public int Compare(T? x, T? y)
        {
            return _comparer.Compare(y, x);
        }
    }
}
