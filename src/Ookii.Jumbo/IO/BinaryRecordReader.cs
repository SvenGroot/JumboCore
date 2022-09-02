// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Runtime.Serialization;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A record reader that reads from a stream created with a <see cref="BinaryRecordWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of the record. Must implement <see cref="IWritable"/> or have an associated <see cref="IValueWriter{T}"/> implementation.</typeparam>
    /// <remarks>
    /// <para>
    ///   No attempt is made to verify that the stream contains the correct type of record. The stream
    ///   must contain records of type <typeparamref name="T"/>. They may not be of a type derived
    ///   from <typeparamref name="T"/>.
    /// </para>
    /// <para>
    ///   This class cannot be used to read starting from any offset other than zero or a structural
    ///   boundary in a record aware stream with the <see cref="RecordStreamOptions.DoNotCrossBoundary"/> option set, because a file created
    ///   with <see cref="BinaryRecordWriter{T}"/> does not contain any record boundaries that can be used
    ///   to sync the file when starting at a random offset.
    /// </para>
    /// </remarks>
    public class BinaryRecordReader<T> : StreamRecordReader<T>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BinaryRecordReader<>));

        private BinaryReader _reader;
        private readonly bool _inputContainsRecordSizes;
        private readonly T _record;
        private readonly bool _allowRecordReuse;
        private readonly string _fileName;
        private readonly bool _deleteFile;
        private readonly long _end;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class that reads from the specified file.
        /// </summary>
        /// <param name="fileName">The path to the file to read from.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the reader can reuse the same instance of <typeparamref name="T"/> every time; <see langword="false"/>
        /// if a new instance must be created for every record.</param>
        /// <param name="deleteFile"><see langword="true"/> if the file should be deleted after reading is finished; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The size of the buffer to use when reading the file.</param>
        /// <param name="compressionType">The type of compression to use to decompress the file.</param>
        /// <param name="uncompressedSize">The uncompressed size of the stream.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public BinaryRecordReader(string fileName, bool allowRecordReuse, bool deleteFile, int bufferSize, CompressionType compressionType, long uncompressedSize)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize).CreateDecompressor(compressionType, uncompressedSize), allowRecordReuse)
        {
            _fileName = fileName;
            _deleteFile = deleteFile;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class that doesn't reuse records.
        /// </summary>
        /// <param name="stream">The stream to read the records from.</param>
        public BinaryRecordReader(Stream stream)
            : this(stream, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader{T}"/> class.
        /// </summary>
        /// <param name="stream">The stream to read the records from.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the reader can reuse the same instance of <typeparamref name="T"/> every time; <see langword="false"/>
        /// if a new instance must be created for every record.</param>
        public BinaryRecordReader(Stream stream, bool allowRecordReuse)
            : this(stream, 0, stream == null ? 0 : stream.Length, allowRecordReuse, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="size">The size.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> to [allow record reuse]; otherwise, <see langword="false"/>.</param>
        public BinaryRecordReader(Stream stream, long offset, long size, bool allowRecordReuse)
            : this(stream, offset, size, allowRecordReuse, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordReader&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="size">The size.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> to [allow record reuse]; otherwise, <see langword="false"/>.</param>
        /// <param name="inputContainsRecordSizes">If set to <see langword="true"/> the input data contains the sizes of the records in between the record data.</param>
        public BinaryRecordReader(Stream stream, long offset, long size, bool allowRecordReuse, bool inputContainsRecordSizes)
            : base(stream, offset, size, true)
        {
            if (offset != 0)
            {
                if (RecordInputStream == null || (RecordInputStream.RecordOptions & RecordStreamOptions.DoNotCrossBoundary) != RecordStreamOptions.DoNotCrossBoundary ||
                    RecordInputStream.OffsetFromBoundary(offset) != 0)
                {
                    throw new ArgumentException("BinaryRecordReader only supports offsets that are zero or at the start of a block if RecordStreamOptions.DoNotCrossBoundary is enabled.", nameof(offset));
                }
            }

            _reader = new BinaryReader(stream);
            // IValueWriter{T} doesn't support record reuse to we never set _allowRecordReuse to true in that case.
            if (ValueWriter<T>.Writer == null)
            {
                // T implements IWritable
                if (allowRecordReuse)
                    _record = (T)FormatterServices.GetUninitializedObject(typeof(T));
                _allowRecordReuse = allowRecordReuse;
            }
            _end = offset + size;
            _inputContainsRecordSizes = inputContainsRecordSizes;
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns>The record, or the default value of <typeparamref name="T"/> if it is beyond the end of the stream.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            if ((RecordInputStream != null && RecordInputStream.IsStopped) || Stream.Position >= _end)
            {
                CurrentRecord = default(T);
                Dispose(); // This will delete the file if necessary.
                return false;
            }

            if (_inputContainsRecordSizes)
            {
                // We don't use the record size, as BinaryRecordReader depends on the records being able to figure out their own size.
                // However, we need to skip the size.
                WritableUtility.Read7BitEncodedInt32(_reader);
            }

            if (_allowRecordReuse)
            {
                // _allowRecordReuse can only be true if the type implements IWritable
                ((IWritable)_record).Read(_reader);
                CurrentRecord = _record;
            }
            else
            {
                var record = ValueWriter<T>.ReadValue(_reader);
                CurrentRecord = record;
            }

            return true;
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
                if (_reader != null)
                {
                    ((IDisposable)_reader).Dispose();
                    _reader = null;
                }
            }
            if (_deleteFile)
            {
                try
                {
                    if (File.Exists(_fileName))
                    {
                        File.Delete(_fileName);
                    }
                }
                catch (IOException ex)
                {
                    _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                }
                catch (UnauthorizedAccessException ex)
                {
                    _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                }
            }
        }
    }
}
