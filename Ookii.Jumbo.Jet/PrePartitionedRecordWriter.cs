// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// A record writer for records that have already been partitioned.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public sealed class PrepartitionedRecordWriter<T> : IRecordWriter, IDisposable
    {
        private readonly RecordWriter<T> _baseWriter;
        private readonly PrepartitionedPartitioner<T> _partitioner;
        private readonly bool _ownsBaseWriter;
        private bool _disposed;

        internal PrepartitionedRecordWriter(RecordWriter<T> baseWriter, bool ownsBaseWriter)
        {
            ArgumentNullException.ThrowIfNull(baseWriter);

            _baseWriter = baseWriter;
            // It's possible that the base writer is not a multi record writer if the there are actually no internal partitions.
            var multiWriter = baseWriter as IMultiRecordWriter<T>;
            if (multiWriter != null)
                _partitioner = multiWriter.Partitioner as PrepartitionedPartitioner<T>;

            _ownsBaseWriter = ownsBaseWriter;
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="partition">The partition of the record.</param>
        public void WriteRecord(T record, int partition)
        {
            if (_partitioner != null)
                _partitioner.CurrentPartition = partition;
            _baseWriter.WriteRecord(record);
        }

        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        /// <value></value>
        public int RecordsWritten
        {
            get { return _baseWriter.RecordsWritten; }
        }

        /// <summary>
        /// Gets the number of bytes written to the stream.
        /// </summary>
        /// <value></value>
        public long OutputBytes
        {
            get { return _baseWriter.OutputBytes; }
        }

        /// <summary>
        /// Gets the number of bytes written to the stream after compression, or 0 if the stream was not compressed.
        /// </summary>
        /// <value></value>
        public long BytesWritten
        {
            get { return _baseWriter.BytesWritten; }
        }

        /// <summary>
        /// Gets the time spent writing.
        /// </summary>
        /// <value>
        /// The time spent writing.
        /// </value>
        public TimeSpan WriteTime
        {
            get { return _baseWriter.WriteTime; }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        public void FinishWriting()
        {
            _baseWriter.FinishWriting();
        }


        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
                if (disposing && _ownsBaseWriter)
                {
                    _baseWriter.Dispose();
                }
            }
        }

        void IRecordWriter.WriteRecord(object record)
        {
            throw new NotSupportedException();
        }
    }
}
