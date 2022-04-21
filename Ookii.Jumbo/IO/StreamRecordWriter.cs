// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that write records to a stream.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    public abstract class StreamRecordWriter<T> : RecordWriter<T>
    {
        private readonly IRecordOutputStream _recordOutputStream;
        private readonly long _startPosition;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordWriter{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to which to write the records.</param>
        protected StreamRecordWriter(Stream stream)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));
            Stream = stream;
            _startPosition = stream.Position;
            _recordOutputStream = stream as IRecordOutputStream;
            if (_recordOutputStream != null && _recordOutputStream.RecordOptions == RecordStreamOptions.None)
                _recordOutputStream = null; // No need to waste time calling MarkRecord if there's no record options set.
        }

        /// <summary>
        /// Gets the underlying stream to which this record reader is writing.
        /// </summary>
        public Stream Stream { get; private set; }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The number of bytes written to the output stream.
        /// </value>
        public override long OutputBytes
        {
            get { return _recordOutputStream == null ? Stream.Position - _startPosition : _recordOutputStream.RecordsSize; }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>If compression was used, the number of bytes written to the output after compression; otherwise, the same value as <see cref="OutputBytes"/>.</value>
        public override long BytesWritten
        {
            get
            {
                ICompressor compressionStream = Stream as ICompressor;
                if (compressionStream == null)
                    return Stream.Position - _startPosition;
                else
                    return compressionStream.CompressedBytesWritten;
            }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        /// <remarks>
        /// <para>
        ///   Derived classes should call the base class implementation after they wrote the record to the stream.
        /// </para>
        /// </remarks>
        protected override void WriteRecordInternal(T record)
        {
            if (_recordOutputStream != null)
                _recordOutputStream.MarkRecord();
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="StreamRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                if (Stream != null)
                {
                    Stream.Dispose();
                    Stream = null;
                }
            }
        }
    }
}
