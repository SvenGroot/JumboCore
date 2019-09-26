// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A record writer that writes files in the record file format.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   For more information about the format of record files, see <see cref="RecordFileHeader"/>.
    /// </para>
    /// </remarks>
    public class RecordFileWriter<T> : StreamRecordWriter<T>
    {
        private BinaryWriter _writer;
        private readonly RecordFileHeader _header;
        private long _lastRecordMarkerPosition;
        private static readonly IValueWriter<T> _valueWriter = ValueWriter<T>.Writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordFileWriter{T}"/> class that writes to the specified stream.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        public RecordFileWriter(Stream stream)
            : base(stream)
        {
            if( stream == null )
                throw new ArgumentNullException(nameof(stream));

            _writer = new BinaryWriter(stream);
            _header = new RecordFileHeader(typeof(T), false); // TODO: Make the value of useStrongName configurable.

            ((IWritable)_header).Write(_writer);
            _lastRecordMarkerPosition = stream.Position - RecordFile.RecordMarkerSize;
        }

        /// <summary>
        /// Gets the header of that was read from the record file.
        /// </summary>
        public RecordFileHeader Header
        {
            get { return _header; }
        }

        /// <summary>
        /// Writes the specified record to the stream.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(T record)
        {
            CheckDisposed();

            if( record == null )
                throw new ArgumentNullException(nameof(record));

            WriteRecordMarkerIfNecessary();

            // In the future we might write a record size or something instead, but at the moment we don't really need that.
            _writer.Write(RecordFile.RecordPrefix);
            if( _valueWriter == null )
                ((IWritable)record).Write(_writer);
            else
                _valueWriter.Write(record, _writer);

            base.WriteRecordInternal(record);
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordFileWriter{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
            {
                if( _writer != null )
                {
                    ((IDisposable)_writer).Dispose();
                    _writer = null;
                }
            }
        }

        private void CheckDisposed()
        {
            if( _writer == null )
                throw new ObjectDisposedException("BinaryRecordWriter");
        }

        private void WriteRecordMarkerIfNecessary()
        {
            if( Stream.Position - _lastRecordMarkerPosition >= RecordFile.RecordMarkerInterval )
                WriteRecordMarker();
        }

        private void WriteRecordMarker()
        {
            // The record file reader will read the record prefix and see this prefix instead, which tells it to read a record marker.
            _writer.Write(RecordFile.RecordMarkerPrefix);
            _lastRecordMarkerPosition = Stream.Position;
            _writer.Write(_header.RecordMarker);
        }
    }
}
