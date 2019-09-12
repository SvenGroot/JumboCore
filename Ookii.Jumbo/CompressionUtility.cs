// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides compression functionality for streams.
    /// </summary>
    public static class CompressionUtility
    {
        #region Nested types

        /// <summary>
        /// Adds support for getting compressed and uncompressed size to <see cref="GZipStream"/>, needed because
        /// the file channel uses that to compute metrics and progress.
        /// </summary>
        private class GZipCompressionStream : GZipStream, ICompressor
        {
            private long _bytesWritten;
            private long _bytesRead;
            private long _uncompressedLength;

            public GZipCompressionStream(Stream stream, CompressionMode mode, long uncompressedLength)
                : base(stream, mode)
            {
                _uncompressedLength = uncompressedLength;
            }

            #region ICompressionStream Members

            public long CompressedBytesWritten
            {
                get { return CanWrite ? BaseStream.Length : 0L; }
            }

            public long UncompressedBytesWritten
            {
                get { return _bytesWritten; }
            }

            public long CompressedBytesRead
            {
                get { return CanRead ? BaseStream.Position : 0L; }
            }

            public long UncompressedBytesRead
            {
                get { return _bytesRead; }
            }

            public long CompressedSize
            {
                get { return BaseStream.Length; }
            }

            #endregion

            public override long Length
            {
                get
                {
                    if( CanRead && _uncompressedLength >= 0 )
                        return _uncompressedLength;
                    else if( CanWrite )
                        return _bytesWritten;
                    else
                        return base.Length;
                }
            }

            public override long Position
            {
                get
                {
                    if( CanRead )
                        return _bytesRead;
                    else if( CanWrite )
                        return _bytesWritten;
                    else
                        return base.Position;
                }
                set { base.Position = value; }
            }

            public override void Write(byte[] array, int offset, int count)
            {
                base.Write(array, offset, count);
                _bytesWritten += count;
            }

            public override int Read(byte[] array, int offset, int count)
            {
                int bytesRead = base.Read(array, offset, count);
                _bytesRead += bytesRead;
                return bytesRead;
            }

            public override int ReadByte()
            {
                int result = base.ReadByte();
                if (result >= 0)
                {
                    ++_bytesRead;
                }

                return result;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(CompressionUtility));

        /// <summary>
        /// Creates a compressor for the specified stream.
        /// </summary>
        /// <param name="target">The stream to write the compressed data to.</param>
        /// <param name="type">The type of compression to use.</param>
        /// <returns>A stream that compresses the data according to the specified compression type, or <paramref name="target"/> itself
        /// if <see cref="CompressionType.None"/> was specified.</returns>
        public static Stream CreateCompressor(this Stream target, CompressionType type)
        {
            if( target == null )
                throw new ArgumentNullException("target");

            switch( type )
            {
            case CompressionType.None:
                return target;
            case CompressionType.GZip:
                _log.Debug("Creating GZipStream compressor.");
                return new GZipCompressionStream(target, CompressionMode.Compress, -1L);
            default:
                throw new NotSupportedException("Unsupported compression type.");
            }
        }

        /// <summary>
        /// Creates a decompressor for the specified stream.
        /// </summary>
        /// <param name="source">The stream to read the compressed data from.</param>
        /// <param name="type">The type of compression to use.</param>
        /// <param name="uncompressedSize">The size of the stream's data after compression, or -1 if unknown.</param>
        /// <returns>A stream that decompresses the data according to the specified compression type, or <paramref name="source"/> itself
        /// if <see cref="CompressionType.None"/> was specified.</returns>
        public static Stream CreateDecompressor(this Stream source, CompressionType type, long uncompressedSize)
        {
            if( source == null )
                throw new ArgumentNullException("source");

            switch( type )
            {
            case CompressionType.None:
                return source;
            case CompressionType.GZip:
                _log.DebugFormat("Creating GZipStream decompressor, uncompressed size {0}.", uncompressedSize);
                return new GZipCompressionStream(source, CompressionMode.Decompress, uncompressedSize);
            default:
                throw new NotSupportedException("Unsupported compression type.");
            }
        }
    }
}
