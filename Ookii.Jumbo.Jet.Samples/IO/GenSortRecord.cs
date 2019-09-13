// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.Runtime.InteropServices;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Represents a record created by the gensort program as provided for the GraySort benchmark.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Records for GraySort are 100 bytes in length, of which 10 bytes are the key and the remaining 90 bytes contain the record number and some
    ///   filler data. Because the <see cref="GenSortRecord"/> class is only used for sorting, it makes no attempt to parse any of the data except
    ///   the key.
    /// </para>
    /// <para>
    ///   The records can contain either ASCII data, or arbitrary binary data.
    /// </para>
    /// <para>
    ///   See http://www.hpl.hp.com/hosted/sortbenchmark/ for more details.
    /// </para>
    /// </remarks>
    [RawComparer(typeof(GenSortRecordRawComparer))]
    public sealed class GenSortRecord : IWritable, IComparable<GenSortRecord>
    {
        /// <summary>
        /// The size of each record, in bytes.
        /// </summary>
        public const int RecordSize = 100;
        /// <summary>
        /// The size of the key of the record, in bytes.
        /// </summary>
        public const int KeySize = 10;

        private byte[] _recordBuffer = new byte[RecordSize];

        /// <summary>
        /// Gets the 100 byte buffer containing the entire record.
        /// </summary>
        public byte[] RecordBuffer
        {
            get { return _recordBuffer; }
        }

        /// <summary>
        /// Extracts the key from the record and converts it to a string.
        /// </summary>
        /// <returns>A string representing the key.</returns>
        public string ExtractKey()
        {
            return Encoding.ASCII.GetString(_recordBuffer, 0, KeySize);
        }

        /// <summary>
        /// Extract the key from the record and copies it to a new byte array.
        /// </summary>
        /// <returns>A byte array, of size <see cref="KeySize"/>, containing the key.</returns>
        public byte[] ExtractKeyBytes()
        {
            byte[] result = new byte[KeySize];
            Array.Copy(_recordBuffer, result, KeySize);
            return result;
        }

        /// <summary>
        /// Compares two specified keys and returns an integer that indicates their relationship to one another in the sort order.
        /// </summary>
        /// <param name="left">The first key. If the array contains more than <see cref="KeySize"/> bytes, the remaining bytes are ignored.</param>
        /// <param name="right">The second key. If the array contains more than <see cref="KeySize"/> bytes, the remaining bytes are ignored.</param>
        /// <returns>Zero if the keys are identical; less than zero if <paramref name="left"/> is less than <paramref name="right"/>;
        /// greater than zero if <paramref name="left"/> is greater than <paramref name="right"/>.</returns>
        public static int CompareKeys(byte[] left, byte[] right)
        {
            for( int x = 0; x < GenSortRecord.KeySize; ++x )
            {
                if( left[x] != right[x] )
                    return left[x] - right[x];
            }
            return 0;
        }

        /// <summary>
        /// Compares two specified partial keys and returns an integer that indicates their relationship to one another in the sort order.
        /// </summary>
        /// <param name="left">The first key. The array must contain only the key, and no further data.</param>
        /// <param name="right">The second key. The array must contain only the key, and no further data.</param>
        /// <returns>Zero if the keys are identical; less than zero if <paramref name="left"/> is less than <paramref name="right"/>;
        /// greater than zero if <paramref name="left"/> is greater than <paramref name="right"/>.</returns>
        public static int ComparePartialKeys(byte[] left, byte[] right)
        {
            int length = Math.Min(left.Length, right.Length);
            for( int x = 0; x < length; ++x )
            {
                if( left[x] != right[x] )
                    return left[x] - right[x];
            }
            return left.Length - right.Length;
        }

        #region IWritable Members

        /// <summary>
        /// Writes the <see cref="GenSortRecord"/> to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="System.IO.BinaryWriter"/> to serialize the object to.</param>
        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(_recordBuffer, 0, RecordSize);
        }

        /// <summary>
        /// Reads the <see cref="GenSortRecord"/> from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="System.IO.BinaryReader"/> to deserialize the object from.</param>
        public void Read(System.IO.BinaryReader reader)
        {
            // _recordBuffer can be null because the ctor isn't called if this instance was created by a record reader.
            if( _recordBuffer == null )
                _recordBuffer = new byte[RecordSize];
            reader.Read(_recordBuffer, 0, RecordSize);
        }

        #endregion

        #region IComparable<GenSortRecord> Members

        /// <summary>
        /// Compares this instance with a specified other <see cref="GenSortRecord"/> and returns an integer that indicates whether this
        /// instance precedes, follows, or appears in the same position in the sort order as the specified <see cref="GenSortRecord"/>.
        /// </summary>
        /// <param name="other">The <see cref="GenSortRecord"/> to compare to.</param>
        /// <returns>Zero if this instance is equal to <paramref name="other"/>; less than zero if this instance precedes <paramref name="other"/>;
        /// greater than zero if this instance follows <paramref name="other"/>.</returns>
        public int CompareTo(GenSortRecord other)
        {
            if( other == null )
                return 1;

            return CompareKeys(_recordBuffer, other._recordBuffer);
        }

        #endregion
    }
}
