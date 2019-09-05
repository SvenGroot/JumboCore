﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides extension methods for <see cref="Stream"/>
    /// </summary>
    public static class StreamExtensions
    {
        /// <summary>
        /// Copies one stream to another.
        /// </summary>
        /// <param name="source">The stream to copy from.</param>
        /// <param name="destination">The stream to copy to.</param>
        public static void CopyTo(this Stream source, Stream destination)
        {
            CopyTo(source, destination, 4096);
        }

        /// <summary>
        /// Copies one stream to another using the specified buffer size.
        /// </summary>
        /// <param name="source">The stream to copy from.</param>
        /// <param name="destination">The stream to copy to.</param>
        /// <param name="bufferSize">The size of the buffer to use while copying.</param>
        public static void CopyTo(this Stream source, Stream destination, int bufferSize)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            if( destination == null )
                throw new ArgumentNullException("destination");
            byte[] buffer = new byte[bufferSize];
            int bytesRead = 0;
            do
            {
                bytesRead = source.Read(buffer, 0, buffer.Length);
                if( bytesRead > 0 )
                {
                    destination.Write(buffer, 0, bytesRead);
                }
            } while( bytesRead > 0 );
        }

        /// <summary>
        /// Copies the specified number of bytes from one stream to another using the default buffer size.
        /// </summary>
        /// <param name="source">The stream to copy from.</param>
        /// <param name="destination">The stream to copy to.</param>
        /// <param name="size">The total number of bytes to copy.</param>
        public static void CopySize(this Stream source, Stream destination, long size)
        {
            CopySize(source, destination, size, 4096);
        }

        /// <summary>
        /// Copies the specified number of bytes from one stream to another using the specified buffer size.
        /// </summary>
        /// <param name="source">The stream to copy from.</param>
        /// <param name="destination">The stream to copy to.</param>
        /// <param name="size">The total number of bytes to copy.</param>
        /// <param name="bufferSize">The size of the buffer to use while copying.</param>
        public static void CopySize(this Stream source, Stream destination, long size, int bufferSize)
        {
            if( source == null )
                throw new ArgumentNullException("source");
            if( destination == null )
                throw new ArgumentNullException("destination");
            byte[] buffer = new byte[bufferSize];
            long bytesLeft = size;
            while( bytesLeft > 0 )
            {
                int bytesRead = source.Read(buffer, 0, (int)Math.Min(buffer.Length, bytesLeft));
                if( bytesRead == 0 )
                {
                    throw new EndOfStreamException("Reached end of stream before specified number of bytes was copied.");
                }
                destination.Write(buffer, 0, bytesRead);
                bytesLeft -= bytesRead;
            }
        }
    }
}
