// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Computes the CRC32 checksum for the input data.
    /// </summary>
    public sealed class Crc32Checksum
    {
        private uint _crc;

        /// <summary>
        /// Gets or sets the the current CRC32 checksum computed so far.
        /// </summary>
        /// <value>
        /// The CRC32 checksum computed so far.
        /// </value>
        public long Value
        {
            get { return _crc; }
            set { _crc = (uint)value; }
        }

        /// <summary>
        /// Gets or sets the the current CRC32 checksum computed so far.
        /// </summary>
        /// <value>
        /// The CRC32 checksum computed so far.
        /// </value>
        [CLSCompliant(false)]
        public uint ValueUInt32
        {
            get { return _crc; }
            set { _crc = value; }
        }

        /// <summary>
        /// Resets the checksum value.
        /// </summary>
        public void Reset()
        {
            _crc = 0;
        }

        /// <summary>
        /// Updates the checksum using the data in the specified array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        public void Update(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            Update(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Updates the checksum using the data in the specified array.
        /// </summary>
        /// <param name="buffer">An array of bytes.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin calculating the checksum.</param>
        /// <param name="count">The number of bytes from <paramref name="buffer"/> to be used in the checksum calculation.</param>
        public void Update(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (offset + count > buffer.Length)
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            _crc = Force.Crc32.Crc32Algorithm.Append(_crc, buffer, offset, count);
        }
    }
}
