// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Reads records from a stream using line breaks as the record boundary.
    /// </summary>
    public sealed class LineRecordReader : StreamRecordReader<Utf8String>
    {
        #region Nested types

        // Unfortunately we cannot use StreamReader because with the buffering it does we cannot
        // accurately tell if we've passed beyond the end of the split.
        private class LineReader
        {
            private Stream _stream;
            private byte[] _buffer;
            private int _bufferPos;
            private int _bufferLength;
            private readonly Utf8String _line = new Utf8String();

            public LineReader(Stream stream, int bufferSize)
            {
                if( bufferSize < 3 )
                    bufferSize = 3; // Needed to check BOM
                _stream = stream;
                _buffer = new byte[bufferSize];
             }

            public Utf8String Line
            {
                get { return _line; }
            }

            public bool CheckByteOrderMark()
            {
                if( _bufferPos == _bufferLength && !ReadBuffer() )
                    return false;

                if( _bufferLength - _bufferPos > 2 && _buffer[_bufferPos] == 0xEF && _buffer[_bufferPos + 1] == 0xBB && _buffer[_bufferPos + 2] == 0xBF )
                {
                    _bufferPos += 3;
                    return true;
                }

                return false;
            }

            private bool ReadBuffer()
            {
                _bufferPos = 0;
                _bufferLength = _stream.Read(_buffer, 0, _buffer.Length);
                return _bufferLength - _bufferPos > 0;
            }

            public void ReadLine(out int bytesProcessed)
            {
                bytesProcessed = 0;
                _line.ByteLength = 0;
                int length;
                while( true )
                {
                    if( _bufferPos == _bufferLength )
                    {
                        if( !ReadBuffer() )
                        {
                            break;
                        }
                    }
                    int start = _bufferPos;
                    for( ; _bufferPos < _bufferLength; ++_bufferPos )
                    {
                        byte b = _buffer[_bufferPos];
                        switch( b )
                        {
                        case (byte)'\r':
                        case (byte)'\n':
                            length = _bufferPos - start;
                            bytesProcessed += length;
                            _line.Append(_buffer, start, length);
                            ++_bufferPos;
                            ++bytesProcessed;
                            if( b == '\r' && (_bufferPos < _bufferLength || ReadBuffer()) && _buffer[_bufferPos] == '\n' )
                            {
                                ++bytesProcessed;
                                ++_bufferPos;
                            }
                            return;
                        }
                    }

                    length = _bufferPos - start;
                    bytesProcessed += length;
                    if( length > 0 )
                    {
                        _line.Append(_buffer, start, length);
                    }
                }
            }
        }

        #endregion

        private const int _bufferSize = 4096;
        private LineReader _reader;
        private long _position;
        private long _end;
        private bool _allowRecordReuse;

        /// <summary>
        /// Initializes a new instance of the <see cref="LineRecordReader"/> class that reads from the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public LineRecordReader(Stream stream)
            : this(stream, 0, stream.Length, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LineRecordReader"/> class that reads the specified range from the specified stream.
        /// </summary>
        /// <param name="stream">The stream to read from.</param>
        /// <param name="offset">The position in the stream to start reading.</param>
        /// <param name="size">The number of bytes to read from the stream.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may re-use the same <see cref="Utf8String"/> instance for every
        /// record; <see langword="false"/> if it must create a new instance for every record.</param>
        /// <remarks>
        /// <para>
        ///   The reader will start reading at <paramref name="offset"/>. If <paramref name="offset"/> is larger than zero, the reader
        ///   will seek ahead to the start of the next record and read from there.
        /// </para>
        /// <para>
        ///   The reader will read a whole number of records until the start of the next record falls
        ///   after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
        ///   read more than <paramref name="size"/> bytes.
        /// </para>
        /// <para>
        ///   The reader will check if <paramref name="stream"/> implements <see cref="IRecordInputStream"/> with <see cref="IRecordInputStream.RecordOptions"/>
        ///   set to <see cref="RecordStreamOptions.DoNotCrossBoundary"/>. If so, if <paramref name="offset"/> falls on a structural boundary
        ///   the reader will not seek ahead because it knows structural boundaries are record boundaries; if <paramref name="offset"/> + <paramref name="size"/> falls
        ///   on a structural boundary, the reader is guaranteed to not read beyond <paramref name="offset"/> + <paramref name="size"/>.
        /// </para>
        /// </remarks>
        public LineRecordReader(Stream stream, long offset, long size, bool allowRecordReuse)
            : base(stream, offset, size)
        {
            if( stream == null )
                throw new ArgumentNullException(nameof(stream));
            _reader = new LineReader(stream, _bufferSize);
            _position = offset;
            _end = offset + size;
            _allowRecordReuse = allowRecordReuse;
            if( _end == stream.Length )
                --_end;
            if( offset == 0 )
            {
                if( _reader.CheckByteOrderMark() )
                {
                    _position += 3;
                    FirstRecordOffset = _position;
                }
            }
            else
            {
                if( RecordInputStream == null || (RecordInputStream.RecordOptions & RecordStreamOptions.DoNotCrossBoundary) != RecordStreamOptions.DoNotCrossBoundary ||
                    RecordInputStream.OffsetFromBoundary(offset) != 0 )
                {
                    ReadRecordInternal();
                    CurrentRecord = null;
                    FirstRecordOffset = _position;
                }
            }
        }

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
                return _position - FirstRecordOffset;
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            if( _position > _end )
            {
                CurrentRecord = null;
                return false;
            }
            int bytesProcessed;
            _reader.ReadLine(out bytesProcessed);

            // If the stream uses RecordStreamOptions.DoNotCrossBoundary, we can run out of data before _position > _end, so check that here.
            if( _reader.Line.ByteLength == 0 && bytesProcessed == 0 )
            {
                CurrentRecord = null;
                return false;
            }

            if( _allowRecordReuse )
                CurrentRecord = _reader.Line;
            else
                CurrentRecord = new Utf8String(_reader.Line);

            _position += bytesProcessed;
            return true;
        }
    }
}
