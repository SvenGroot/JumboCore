// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides methods to read primitive types from an array of bytes in a system independent format.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This class can be used to aid in implementing raw comparers for your record types.
    /// </para>
    /// <para>
    ///     Unlike the <see cref="BitConverter"/> class, this class always uses little endian formatting. It can be used to read data written by the <see cref="System.IO.BinaryWriter"/> class.
    /// </para>
    /// </remarks>
    public static class LittleEndianBitConverter
    {
        /// <summary>
        /// Reads a 16-bit signed integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 16-bit signed integer formed by two bytes beginning at <paramref name="offset"/>.</returns>
        public static short ToInt16(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 2 )
                throw new ArgumentOutOfRangeException("offset");
            return (short)(buffer[offset] | (buffer[offset + 1] << 8));
        }

        /// <summary>
        /// Reads a 16-bit unsigned integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 16-bit unsigned integer formed by two bytes beginning at <paramref name="offset"/>.</returns>
        [CLSCompliant(false)]
        public static ushort ToUInt16(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 2 )
                throw new ArgumentOutOfRangeException("offset");
            return (ushort)(buffer[offset] | (buffer[offset + 1] << 8));
        }

        /// <summary>
        /// Reads a 32-bit signed integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 32-bit signed integer formed by four bytes beginning at <paramref name="offset"/>.</returns>
        public static int ToInt32(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 4 )
                throw new ArgumentOutOfRangeException("offset");
            return (buffer[offset]) | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24);
        }

        /// <summary>
        /// Reads a 32-bit unsigned integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 32-bit unsigned integer formed by four bytes beginning at <paramref name="offset"/>.</returns>
        [CLSCompliant(false)]
        public static uint ToUInt32(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 4 )
                throw new ArgumentOutOfRangeException("offset");
            return (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24));
        }

        /// <summary>
        /// Reads a 64-bit signed integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 64-bit signed integer formed by eight bytes beginning at <paramref name="offset"/>.</returns>
        public static long ToInt64(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 8 )
                throw new ArgumentOutOfRangeException("offset");
            uint low = (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24));
            uint high = (uint)(buffer[offset + 4] | (buffer[offset + 5] << 8) | (buffer[offset + 6] << 16) | (buffer[offset + 7] << 24));
            return ((long)high << 32) | low;
        }

        /// <summary>
        /// Reads a 64-bit unsigned integer from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A 64-bit unsigned integer formed by eight bytes beginning at <paramref name="offset"/>.</returns>
        [CLSCompliant(false)]
        public static ulong ToUInt64(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 8 )
                throw new ArgumentOutOfRangeException("offset");
            uint low = (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24));
            uint high = (uint)(buffer[offset + 4] | (buffer[offset + 5] << 8) | (buffer[offset + 6] << 16) | (buffer[offset + 7] << 24));
            return ((ulong)high << 32) | low;
        }

        /// <summary>
        /// Reads a single-precision floating point number from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A single-precision floating point number formed by four bytes beginning at <paramref name="offset"/>.</returns>
        public static unsafe float ToSingle(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 4 )
                throw new ArgumentOutOfRangeException("offset");
            uint bits = (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24));
            return *(float*)&bits;
        }

        /// <summary>
        /// Reads a double-precision floating point number from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A double-precision floating point number formed by four bytes beginning at <paramref name="offset"/>.</returns>
        public static unsafe double ToDouble(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 8 )
                throw new ArgumentOutOfRangeException("offset");
            uint low = (uint)(buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24));
            uint high = (uint)(buffer[offset + 4] | (buffer[offset + 5] << 8) | (buffer[offset + 6] << 16) | (buffer[offset + 7] << 24));
            ulong bits = ((ulong)high << 32) | low;
            return *(double*)&bits;
        }

        /// <summary>
        /// Reads a decimal value from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A decimal value formed by sixteen bytes beginning at <paramref name="offset"/>.</returns>
        public static decimal ToDecimal(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 16 )
                throw new ArgumentOutOfRangeException("offset");
            int[] bits = new[] 
            { 
                (buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24)),
                (buffer[offset + 4] | (buffer[offset + 5] << 8) | (buffer[offset + 6] << 16) | (buffer[offset + 7] << 24)),
                (buffer[offset + 8] | (buffer[offset + 9] << 8) | (buffer[offset + 10] << 16) | (buffer[offset + 11] << 24)),
                (buffer[offset + 12] | (buffer[offset + 13] << 8) | (buffer[offset + 14] << 16) | (buffer[offset + 15] << 24))
            };
            return new decimal(bits);
        }

        /// <summary>
        /// Reads a date and time from the specified position in a byte array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>.</param>
        /// <returns>A date and time formed by twelve bytes beginning at <paramref name="offset"/>.</returns>
        public static DateTime ToDateTime(byte[] buffer, int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset > buffer.Length - 12 )
                throw new ArgumentOutOfRangeException("offset");
            DateTimeKind kind = (DateTimeKind)ToInt32(buffer, offset);
            long ticks = ToInt64(buffer, offset + 4);
            return new DateTime(ticks, kind);
        }

        /// <summary>
        /// Reads in a 32-bit integer in compressed format.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The starting position within <paramref name="buffer"/>. On return, contains the offset after the value.</param>
        /// <returns>A 32-bit integer in compressed format, using between one and five bytes. </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1045:DoNotPassTypesByReference", MessageId = "1#")]
        public static int ToInt32From7BitEncoding(byte[] buffer, ref int offset)
        {
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 || offset >= buffer.Length )
                throw new ArgumentOutOfRangeException("offset");
            byte currentByte;
            int result = 0;
            int bits = 0;
            do
            {
                if( bits == 35 )
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = buffer[offset++];
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while( (currentByte & 0x80) != 0 );
            return result;
        }
    }
}
