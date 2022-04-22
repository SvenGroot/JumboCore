// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Runtime.InteropServices;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Represents a buffer of memory stored on the unmanaged (native) heap.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This class is used as an alternative for byte[]. The main differences are that the memory is returned
    ///   to the OS when the class is disposed/finalized, and the buffer is not zero-initialized.
    /// </para>
    /// <para>
    ///   The main reason for the existence of this class is Mono's reluctance to release memory from the managed heap
    ///   back to the OS which can lead to pagefile thrashing if you're dealing with many large buffers. This problem
    ///   may not apply to the sgen garbage collector, but this has not been tested.
    /// </para>
    /// </remarks>
    public sealed unsafe class UnmanagedBuffer : IDisposable
    {
        private byte* _buffer;

        /// <summary>
        /// Initializes a new instance of the <see cref="UnmanagedBuffer"/> class.
        /// </summary>
        /// <param name="size">The size, in bytes, of the buffer.</param>
        public UnmanagedBuffer(int size)
        {
            _buffer = (byte*)Marshal.AllocHGlobal(size);
            Size = size;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnmanagedBuffer"/> class.
        /// </summary>
        /// <param name="size">The size, in bytes, of the buffer.</param>
        public UnmanagedBuffer(long size)
        {
            _buffer = (byte*)Marshal.AllocHGlobal(new IntPtr(size));
            Size = size;
        }

        /// <summary>
        /// Releases all resources used by this class.
        /// </summary>
        ~UnmanagedBuffer()
        {
            DisposeInternal();
        }

        /// <summary>
        /// Gets the size of the buffer.
        /// </summary>
        /// <value>
        /// The size of the buffer, in bytes.
        /// </value>
        public long Size { get; private set; }

        /// <summary>
        /// Gets a pointer to the first byte of the buffer.
        /// </summary>
        /// <value>
        /// An unsafe pointer to the first byte of the buffer
        /// </value>
        [CLSCompliant(false)]
        public byte* Buffer
        {
            get { return _buffer; }
        }

        /// <summary>
        /// Copies data from a managed array to the buffer.
        /// </summary>
        /// <param name="source">The managed byte array containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The <see cref="UnmanagedBuffer"/> to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        public static void Copy(byte[] source, int sourceIndex, UnmanagedBuffer destination, int destinationIndex, int count)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            if (destinationIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(destinationIndex));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (sourceIndex + count > source.Length)
                throw new ArgumentException("sourceIndex + count is larger than the source array.");
            if (destinationIndex + count > destination.Size)
                throw new ArgumentException("destinationIndex + count is larger than the destination array.");

            destination.CheckDisposed();

            Marshal.Copy(source, sourceIndex, new IntPtr(destination._buffer + destinationIndex), count);
        }

        /// <summary>
        /// Copies data from a managed array to the buffer, wrapping around if necessary.
        /// </summary>
        /// <param name="source">The managed byte array containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The <see cref="UnmanagedBuffer"/> to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        /// <returns>The next index position after writing the data.</returns>
        public static long CopyCircular(byte[] source, int sourceIndex, UnmanagedBuffer destination, long destinationIndex, int count)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            if (destinationIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(destinationIndex));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (sourceIndex + count > source.Length)
                throw new ArgumentException("sourceIndex + count is larger than the source array.");
            var end = destinationIndex + count;
            if (end > destination.Size)
            {
                end %= destination.Size;
                if (end > destinationIndex)
                    throw new ArgumentException("count is larger than the destination array.");
            }

            destination.CheckDisposed();

            if (end >= destinationIndex)
            {
                Marshal.Copy(source, sourceIndex, new IntPtr(destination._buffer + destinationIndex), count);
            }
            else
            {
                // Because count is an int, if this condition is true the two casts here will never overflow
                var firstCount = (int)(destination.Size - destinationIndex);
                Marshal.Copy(source, sourceIndex, new IntPtr(destination._buffer + destinationIndex), firstCount);
                Marshal.Copy(source, sourceIndex + firstCount, new IntPtr(destination._buffer), (int)end);
            }
            return end % destination.Size;
        }

        /// <summary>
        /// Copies data from a buffer to a managed array.
        /// </summary>
        /// <param name="source">The <see cref="UnmanagedBuffer"/> containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The managed byte array to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        public static void Copy(UnmanagedBuffer source, int sourceIndex, byte[] destination, int destinationIndex, int count)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            if (destinationIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(destinationIndex));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (sourceIndex + count > source.Size)
                throw new ArgumentException("sourceIndex + count is larger than the source array.");
            if (destinationIndex + count > destination.Length)
                throw new ArgumentException("destinationIndex + count is larger than the destination array.");

            source.CheckDisposed();

            Marshal.Copy(new IntPtr(source._buffer + sourceIndex), destination, destinationIndex, count);
        }

        /// <summary>
        /// Copies data from a circular buffer to a managed array.
        /// </summary>
        /// <param name="source">The <see cref="UnmanagedBuffer"/> containing the data to copy.</param>
        /// <param name="sourceIndex">The index in <paramref name="source"/> to start copying at.</param>
        /// <param name="destination">The managed byte array to copy the data to.</param>
        /// <param name="destinationIndex">The index in <paramref name="destination"/> to start copying at.</param>
        /// <param name="count">The number of bytes to copy.</param>
        /// <returns>The next index position after writing the data.</returns>
        public static long CopyCircular(UnmanagedBuffer source, long sourceIndex, byte[] destination, int destinationIndex, int count)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (sourceIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(sourceIndex));
            if (destinationIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(destinationIndex));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (destinationIndex + count > destination.Length)
                throw new ArgumentException("destinationIndex + count is larger than the destination array.");

            var end = sourceIndex + count;
            if (end > source.Size)
            {
                end %= source.Size;
                if (end > sourceIndex)
                    throw new ArgumentException("count is larger than the source array.");
            }

            source.CheckDisposed();

            if (end >= sourceIndex)
            {
                Marshal.Copy(new IntPtr(source._buffer + sourceIndex), destination, destinationIndex, count);
            }
            else
            {
                var firstCount = (int)(source.Size - sourceIndex);
                Marshal.Copy(new IntPtr(source._buffer + sourceIndex), destination, destinationIndex, firstCount);
                Marshal.Copy(new IntPtr(source._buffer), destination, destinationIndex + firstCount, (int)end);
            }
            return end % source.Size;

        }

        /// <summary>
        /// Resizes the buffer.
        /// </summary>
        /// <param name="size">The new size of the buffer.</param>
        public void Resize(int size)
        {
            CheckDisposed();
            _buffer = (byte*)Marshal.ReAllocHGlobal(new IntPtr(_buffer), new IntPtr(size));
        }

        /// <summary>
        /// Releases all resources used by this class.
        /// </summary>
        public void Dispose()
        {
            DisposeInternal();
            GC.SuppressFinalize(this);
        }

        private void DisposeInternal()
        {
            if (_buffer != null)
            {
                Marshal.FreeHGlobal(new IntPtr(_buffer));
                _buffer = null;
            }
        }

        private void CheckDisposed()
        {
            if (_buffer == null)
                throw new ObjectDisposedException(typeof(UnmanagedBuffer).FullName);
        }
    }
}
