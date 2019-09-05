// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Quicksort implementation for indexed binary buffers.
    /// </summary>
    public static class IndexedQuicksort
    {
        /// <summary>
        /// Sorts the specified indexed data.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <param name="buffer">The buffer containing the data.</param>
        /// <param name="comparer">The <see cref="IRawComparer{T}"/> for the records in the buffer.</param>
        public static void Sort<T>(RecordIndexEntry[] index, byte[] buffer, IRawComparer<T> comparer)
        {
            if( index == null )
                throw new ArgumentNullException("index");
            Sort(index, buffer, comparer, 0, index.Length);
        }

        /// <summary>
        /// Sorts the specified indexed data.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <param name="buffer">The buffer containing the data.</param>
        /// <param name="comparer">The <see cref="IRawComparer{T}"/> for the records in the buffer.</param>
        /// <param name="offset">The offset into <paramref name="index"/> of the first item to sort.</param>
        /// <param name="count">The number of items in <paramref name="index"/> starting at <paramref name="offset"/> to sort.</param>
        public static void Sort<T>(RecordIndexEntry[] index, byte[] buffer, IRawComparer<T> comparer, int offset, int count)
        {
            if( index == null )
                throw new ArgumentNullException("index");
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( comparer == null )
                throw new ArgumentNullException("comparer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > index.Length )
                throw new ArgumentException("The sum of offset and count is greater than the index length.");
            SortCore(buffer, index, comparer, offset, offset + count);
        }

        private static void SortCore<T>(byte[] buffer, RecordIndexEntry[] index, IRawComparer<T> comparer, int left, int right)
        {
            while( true )
            {
                int i;
                int j;
                if( right - left < 13 )
                {
                    // Perform insertion sort on small array.
                    for( i = left; i < right; ++i )
                    {
                        for( j = i; j > left && Compare(buffer, index, comparer, j - 1, j) > 0; --j )
                        {
                            Swap(index, j, j - 1);
                        }
                    }
                    return;
                }

                // Assure left, pivot, right are in the right order.
                Order(buffer, index, comparer, (left + right) >> 1, left);
                Order(buffer, index, comparer, (left + right) >> 1, right - 1);
                Order(buffer, index, comparer, left, right - 1);

                i = left;
                j = right;
                int ll = left;
                int rr = right;
                int cr;
                while( true )
                {
                    while( ++i < j )
                    {
                        if( (cr = Compare(buffer, index, comparer, i, left)) > 0 ) 
                            break;
                        if( 0 == cr && ++ll != i )
                            Swap(index, ll, i);
                    }
                    while( --j > i )
                    {
                        if( (cr = Compare(buffer, index, comparer, left, j)) > 0 ) 
                            break;
                        if( 0 == cr && --rr != j )
                            Swap(index, rr, j);
                    }
                    if( i < j ) 
                        Swap(index, i, j);
                    else 
                        break;
                }
                j = i;
                // swap pivot- and all eq values- into position
                while( ll >= left )
                    Swap(index, ll--, --i);
                while( rr < right )
                    Swap(index, rr++, j++);

                // Conquer
                // Recurse on smaller interval first to keep stack shallow
                Debug.Assert(i != j);
                if( i - left < right - j )
                {
                    SortCore(buffer, index, comparer, left, i);
                    left = j;
                }
                else
                {
                    SortCore(buffer, index, comparer, j, right);
                    right = i;
                }
            }
        }

        private static int Compare<T>(byte[] buffer, RecordIndexEntry[] s, IRawComparer<T> comparer, int p, int r)
        {
            return comparer.Compare(buffer, s[p].Offset, s[p].Count, buffer, s[r].Offset, s[r].Count);
        }

        private static void Swap(RecordIndexEntry[] s, int p, int r)
        {
            RecordIndexEntry temp = s[p];
            s[p] = s[r];
            s[r] = temp;
        }

        private static void Order<T>(byte[] buffer, RecordIndexEntry[] s, IRawComparer<T> comparer, int p, int r)
        {
            if( Compare(buffer, s, comparer, p, r) > 0 )
                Swap(s, p, r);
        }
    }
}
