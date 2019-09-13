// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Memory stream that uses an <see cref="UnmanagedBuffer"/> for the underlying storage.
    /// </summary>
    [CLSCompliant(false)]
    public sealed class UnmanagedBufferMemoryStream : UnmanagedMemoryStream
    {
        private UnmanagedBuffer _buffer;

        /// <summary>
        /// Event raised when the stream is disposed
        /// </summary>
        public event EventHandler Disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="UnmanagedBufferMemoryStream"/>.
        /// </summary>
        /// <param name="capacity">The capacity of the memory stream.</param>
        public UnmanagedBufferMemoryStream(long capacity)
        {
            _buffer = new UnmanagedBuffer(capacity);
            unsafe
            {
                Initialize(_buffer.Buffer, 0, capacity, FileAccess.ReadWrite);
            }
            InitialCapacity = capacity;
        }

        /// <summary>
        /// Gets the capacity the stream had when it was created.
        /// </summary>
        public long InitialCapacity { get; private set; }

        /// <summary>
        /// Releases all resources used by the <see cref="UnmanagedBufferMemoryStream"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release unmanaged and managed resources; <see langword="false"/> to release unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( _buffer != null && disposing )
            {
                _buffer.Dispose();
                _buffer = null;
                OnDisposed(EventArgs.Empty);
            }
        }

        /// <summary>
        /// Raises the <see cref="Disposed"/> event.
        /// </summary>
        /// <param name="e">The data for the event.</param>
        private void OnDisposed(EventArgs e)
        {
            EventHandler handler = Disposed;
            if( handler != null )
                handler(this, e);
        }
    }
}
