// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes that read records from a stream or part of a stream.
    /// </summary>
    /// <typeparam name="T">The type of the records to read.</typeparam>
    /// <remarks>
    /// <para>
    ///   Deriving classes should start reading at <see cref="Offset"/>. If <see cref="Offset"/> is not on a record boundary,
    ///   they should seek ahead to the first record boundary and start from there.
    /// </para>
    /// <para>
    ///   Deriving classes should use <see cref="Offset"/> and <see cref="Size"/> to determine when to stop returning records.
    ///   If <see cref="Offset"/> + <see cref="Size"/> is not on a record boundary, they should continue reading until the
    ///   next record boundary.
    /// </para>
    /// <para>
    ///   If the stream implements <see cref="IRecordInputStream"/> with <see cref="RecordStreamOptions.DoNotCrossBoundary"/> set, 
    ///   and <see cref="Offset"/> + <see cref="Size"/> is on a structural boundary,
    ///   <see cref="IRecordInputStream.StopReadingAtPosition"/> will be set to <see cref="Offset"/> + <see cref="Size"/>. In
    ///   this situation, <see cref="System.IO.Stream.Read(Byte[], int, int)"/> may return 0 before <see cref="Size"/> bytes are read. A
    ///   deriving class should check this and stop reading at that point.
    /// </para>
    /// </remarks>
    public abstract class StreamRecordReader<T> : RecordReader<T>
    {
        private bool _disposed;
        private long _bytesRead;
        private long _paddingBytesSkipped;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        protected StreamRecordReader(Stream stream)
            : this(stream, 0, stream.Length)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <remarks>
        /// <para>
        ///   The reader will read a whole number of records until the start of the next record falls
        ///   after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        ///   read more than <paramref name="size"/> bytes.
        /// </para>
        /// <para>
        ///   If the stream implements <see cref="IRecordInputStream"/> with <see cref="RecordStreamOptions.DoNotCrossBoundary"/> set, 
        ///   and <paramref name="offset"/> + <paramref name="size"/> is on a structural boundary,
        ///   <see cref="IRecordInputStream.StopReadingAtPosition"/> will be set to <paramref name="offset"/> + <paramref name="size"/>.
        /// </para>
        /// </remarks>
        protected StreamRecordReader(Stream stream, long offset, long size)
            : this(stream, offset, size, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordReader{T}"/> class with the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <param name="seekToOffset"><see langword="true"/> to seek the stream to <paramref name="offset"/>; <see langword="false"/> to leave the stream at the current position.</param>
        /// <remarks>
        /// <para>
        ///   The reader will read a whole number of records until the start of the next record falls
        ///   after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        ///   read more than <paramref name="size"/> bytes.
        /// </para>
        /// <para>
        ///   If the stream implements <see cref="IRecordInputStream"/> with <see cref="RecordStreamOptions.DoNotCrossBoundary"/> set, 
        ///   and <paramref name="offset"/> + <paramref name="size"/> is on a structural boundary,
        ///   <see cref="IRecordInputStream.StopReadingAtPosition"/> will be set to <paramref name="offset"/> + <paramref name="size"/>.
        /// </para>
        /// </remarks>
        protected StreamRecordReader(Stream stream, long offset, long size, bool seekToOffset)
        {
            ArgumentNullException.ThrowIfNull(stream);
            if (offset < 0 || (offset > 0 && offset >= stream.Length))
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            if (offset + size > stream.Length)
                throw new ArgumentException("Offset + size is beyond the end of the stream.");

            Stream = stream;
            if (seekToOffset && offset != 0) // to prevent NotSupportedException on streams that can't seek.
                Stream.Position = offset;
            Offset = offset;
            FirstRecordOffset = offset;
            Size = size;
            var recordInputStream = Stream as IRecordInputStream;
            if (recordInputStream != null && (recordInputStream.RecordOptions & RecordStreamOptions.DoNotCrossBoundary) == RecordStreamOptions.DoNotCrossBoundary && recordInputStream.OffsetFromBoundary(offset + size) == 0)
                recordInputStream.StopReadingAtPosition = offset + size;
            RecordInputStream = recordInputStream;
        }

        /// <summary>
        /// Gets the position in the stream where reading began.
        /// </summary>
        protected long Offset { get; private set; }

        /// <summary>
        /// Gets the total size to read from the stream.
        /// </summary>
        protected long Size { get; private set; }

        /// <summary>
        /// Gets the underlying stream from which this record reader is reading.
        /// </summary>
        protected Stream Stream { get; private set; }

        /// <summary>
        /// Gets or sets the <see cref="IRecordInputStream"/> implementation of <see cref="Stream"/>.
        /// </summary>
        /// <value><see cref="Stream"/> cast to <see cref="IRecordInputStream"/>, or <see langword="null"/> if it doesn't implement it.</value>
        protected IRecordInputStream RecordInputStream { get; private set; }

        /// <summary>
        /// Gets or sets the offset of the first record.
        /// </summary>
        /// <value>The first record offset.</value>
        /// <remarks>
        /// <para>
        ///   If a deriving record reader seeks to the start of the first record from the specified <see cref="Offset"/>,
        ///   it can set this property once it has found it to correct the value returned by <see cref="InputBytes"/>.
        /// </para>
        /// </remarks>
        protected long FirstRecordOffset { get; set; }

        /// <summary>
        /// Gets the size of the records before deserialization.
        /// </summary>
        /// <value>
        /// The number of bytes read from the stream.
        /// </value>
        public override long InputBytes
        {
            get
            {
                // This property doesn't need to be thread-safe, so it doesn't need the try/catch of UncompressedBytesRead,
                // but it might still be called after the reader is disposed (because BinaryRecordReader disposes itself when the last byte is read).
                var s = Stream;
                if (s == null)
                    return _bytesRead - (FirstRecordOffset - Offset) - _paddingBytesSkipped;
                else
                {
                    var result = s.Position - FirstRecordOffset;
                    if (RecordInputStream != null)
                        result -= RecordInputStream.PaddingBytesSkipped;
                    return result;
                }
            }
        }

        /// <summary>
        /// Gets the size of the records before deserialization.
        /// </summary>
        /// <value>
        /// The size of the records before deserialization, or 0 if the records were not read from a serialized source.
        /// </value>
        public override long BytesRead
        {
            get
            {
                var compressor = Stream as ICompressor;
                if (compressor == null)
                    return UncompressedBytesRead;
                else
                    return compressor.CompressedBytesRead;
            }
        }

        /// <summary>
        /// Gets the progress of the reader.
        /// </summary>
        public override float Progress
        {
            get
            {
                return Math.Min(1.0f, UncompressedBytesRead / (float)Size);
            }
        }

        private long UncompressedBytesRead
        {
            get
            {
                // Progress needs to be thread safe, so this must be as well. But we don't want to lock each usage of Stream.
                // It still might not be entirely safe because Stream isn't thread safe, but it'll have to do for now.
                var s = Stream;
                if (s == null)
                    return _bytesRead;
                else
                {
                    try
                    {
                        return s.Position - Offset;
                    }
                    catch (ObjectDisposedException)
                    {
                        return _bytesRead;
                    }
                }
            }
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="StreamRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (!_disposed)
            {
                if (disposing)
                {
                    if (Stream != null)
                    {
                        _bytesRead = UncompressedBytesRead; // Store so that property can be used after the object is disposed.
                        if (RecordInputStream != null)
                            _paddingBytesSkipped = RecordInputStream.PaddingBytesSkipped;
                        var s = Stream;
                        Stream = null;
                        s.Dispose();
                    }
                }
                _disposed = true;
            }
        }

        /// <summary>
        /// Checks if the object is disposed, and if so throws a <see cref="ObjectDisposedException"/>.
        /// </summary>
        /// <exception cref="ObjectDisposedException">The <see cref="StreamRecordReader{T}"/> was disposed.</exception>
        protected void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException("StreamRecordReader");
        }
    }
}
