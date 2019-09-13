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
    public sealed class Crc32
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Crc32));

        private static bool _useNativeCode = true;
        private uint _crc;

        /// <summary>
        /// Gets or sets a value indicating whether to use the native CRC32 algorithm, if possible.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> to use the native CRC32 algorithm; otherwise, <see langword="false"/>.
        /// 	The default value is <see langword="true"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If the <see cref="UseNativeCode"/> property is set to <see langword="true"/> and the Jumbo native
        ///   library cannot be loaded, the <see cref="Update(byte[],int,int)"/> method will automatically set it to
        ///   <see langword="false"/>.
        /// </para>
        /// </remarks>
        public static bool UseNativeCode
        {
            get { return _useNativeCode; }
            set { _useNativeCode = value; }
        }

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
            if( buffer == null )
                throw new ArgumentNullException("buffer");

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
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( offset < 0 )
                throw new ArgumentOutOfRangeException("offset");
            if( count < 0 )
                throw new ArgumentOutOfRangeException("count");
            if( offset + count > buffer.Length )
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if( _useNativeCode )
            {
                try
                {
                    _crc = NativeMethods.JumboCrc32(buffer, (uint)offset, (uint)count, _crc);
                    return;
                }
                catch( DllNotFoundException ex )
                {
                    _log.WarnFormat("Unable to load Jumbo native library: {0}.", ex.Message);
                    _useNativeCode = false; // Don't try to load the library again
                }
            }

            // Native code failed if we got here
            _crc = Crc32Managed.Calculate(buffer, offset, count, _crc);
        }
    }
}
