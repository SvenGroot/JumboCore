// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Interface for streams that compress data.
    /// </summary>
    public interface ICompressor
    {
        /// <summary>
        /// When compressing, gets the number of compressed bytes written.
        /// </summary>
        /// <value>
        /// The number of compressed bytes written, or 0 if this stream only supports decompression.
        /// </value>
        long CompressedBytesWritten { get; }

        /// <summary>
        /// When compressing, gets the number of uncompressed bytes written.
        /// </summary>
        /// <value>
        /// The number of uncompressed bytes written, or 0 if this stream only supports decompression.
        /// </value>
        long UncompressedBytesWritten { get; }

        /// <summary>
        /// When decompressing, gets the number of compressed bytes read.
        /// </summary>
        /// <value>
        /// The number of compressed bytes read, or 0 if this stream only supports compression.
        /// </value>
        long CompressedBytesRead { get; }

        /// <summary>
        /// When decompressing, gets the number of uncompressed bytes read.
        /// </summary>
        /// <value>
        /// The number of uncompressed bytes read, or 0 if this stream only supports compression.
        /// </value>
        long UncompressedBytesRead { get; }

        /// <summary>
        /// Gets the length of the underlying compressed stream.
        /// </summary>
        /// <value>
        /// The length of the underlying stream.
        /// </value>
        long CompressedSize { get; }
    }
}
