// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides extension methods for various list types.
    /// </summary>
    public static class ListExtensions
    {
        /// <summary>
        /// Randomizes the specified list.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">The list to randomize.</param>
        public static void Randomize<T>(this IList<T> list)
        {
            list.Randomize(new Random());
        }

        /// <summary>
        /// Randomizes the specified list.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">The list to randomize.</param>
        /// <param name="random">The randomizer to use.</param>
        public static void Randomize<T>(this IList<T> list, Random random)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));

            if (random == null)
                throw new ArgumentNullException(nameof(random));
            var n = list.Count;        // The number of items left to shuffle (loop invariant).
            while (n > 1)
            {
                var k = random.Next(n);  // 0 <= k < n.
                n--;                     // n is now the last pertinent index;
                var temp = list[n];     // swap array[n] with array[k] (does nothing if k == n).
                list[n] = list[k];
                list[k] = temp;
            }
        }

        /// <summary>
        /// Creates a string with the items of a list separated by the specified delimiter.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">A list.</param>
        /// <param name="delimiter">The delimiter to use.</param>
        /// <returns>A string containing the delimited list.</returns>
        public static string ToDelimitedString<T>(this IEnumerable<T> list, string delimiter)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));
            if (delimiter == null)
                throw new ArgumentNullException(nameof(delimiter));

            var result = new StringBuilder();
            var first = true;
            foreach (var item in list)
            {
                if (first)
                    first = false;
                else
                    result.Append(delimiter);
                result.Append(item);
            }

            return result.ToString();
        }

        /// <summary>
        /// Creates a string with the items of a list separated by a comma.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="list">A list.</param>
        /// <returns>A string containing the delimited list.</returns>
        public static string ToDelimitedString<T>(this IEnumerable<T> list)
        {
            return list.ToDelimitedString(", ");
        }

        /// <summary>
        /// Gets the hash code for the specified sequence of elements.
        /// </summary>
        /// <typeparam name="T">The type of the elements</typeparam>
        /// <param name="list">A list.</param>
        /// <returns>A hash code for the entire sequence.</returns>
        public static int GetSequenceHashCode<T>(this IEnumerable<T> list)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));
            var hash = 0x218A9B2C;
            foreach (var item in list)
            {
                var itemHash = item.GetHashCode();
                //mix up the bits. 
                hash = itemHash ^ ((hash << 5) + hash);
            }
            return hash;
        }

        /// <summary>
        /// Swaps two elements in the specified list.
        /// </summary>
        /// <typeparam name="T">The type of the elements</typeparam>
        /// <param name="list">The list.</param>
        /// <param name="index1">The first index.</param>
        /// <param name="index2">The second index.</param>
        public static void Swap<T>(this IList<T> list, int index1, int index2)
        {
            if (list == null)
                throw new ArgumentNullException(nameof(list));
            if (index1 != index2)
            {
                var temp = list[index1];
                list[index1] = list[index2];
                list[index2] = temp;
            }
        }

        /// <summary>
        /// Sorts the elements of a sequence in ascending or descending order according to a key.
        /// </summary>
        /// <typeparam name="TElement">The type of the element of <paramref name="source"/>.</typeparam>
        /// <typeparam name="TKey">The type of the key returned by <paramref name="keySelector"/>.</typeparam>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="keySelector">A function to extract a key from an element.</param>
        /// <param name="ascending"><see langword="true"/> to sort in ascending order; <see langword="false"/> to sort in descending order.</param>
        /// <returns>An <see cref="IOrderedEnumerable{TElement}"/> whose elements are sorted according to a key.</returns>
        public static IOrderedEnumerable<TElement> OrderBy<TElement, TKey>(this IEnumerable<TElement> source, Func<TElement, TKey> keySelector, bool ascending)
        {
            if (ascending)
                return source.OrderBy(keySelector);
            else
                return source.OrderByDescending(keySelector);
        }
    }
}
