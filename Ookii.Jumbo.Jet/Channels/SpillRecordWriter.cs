// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Multi record writer that collects records in an in-memory buffer, and periodically spills the record to the output when the buffer is full.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public abstract class SpillRecordWriter<T> : RecordWriter<T>, IMultiRecordWriter<T>
    {
        #region Nested types

        private sealed class CircularBufferStream : Stream
        {
            private readonly byte[] _buffer;
            private int _bufferPos;
            private int _bufferUsed;
            private int _bufferMark;
            private readonly AutoResetEvent _freeBufferEvent = new AutoResetEvent(false);
            private readonly SpillRecordWriter<T> _writer;
            private readonly WaitHandle[] _bufferEvents;

            public CircularBufferStream(SpillRecordWriter<T> writer, int size)
            {
                _writer = writer;
                _buffer = new byte[size];
                _bufferEvents = new WaitHandle[] { _freeBufferEvent, writer._cancelEvent };
            }

            public int BufferPos
            {
                get { return _bufferPos; }
            }

            public int BufferMark
            {
                get { return _bufferMark; }
            }

            public int Size
            {
                get { return _buffer.Length; }
            }

            public byte[] Buffer
            {
                get { return _buffer; }
            }

            public int BufferUsed
            {
                get { return _bufferUsed; }
            }


            public override bool CanRead
            {
                get { return false; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return true; }
            }

            public override void Flush()
            {
            }

            public override long Length
            {
                get { throw new NotSupportedException(); }
            }

            public override long Position
            {
                get
                {
                    throw new NotSupportedException();
                }
                set
                {
                    throw new NotSupportedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                int newBufferUsed = Interlocked.Add(ref _bufferUsed, count);
                while( newBufferUsed > _buffer.Length )
                {
                    _log.InfoFormat("Waiting for buffer space, current buffer pos {0}, buffer used {1}", _bufferPos, newBufferUsed);
                    _writer.RequestOutputSpill();
                    // This is only safe for one thread to use write, but record writers and streams are not thread safe, so no problem
                    // If the cancel event was set while writing, the object was disposed or an error occurred in the spill thread.
                    if( WaitHandle.WaitAny(_bufferEvents) == 1 )
                    {
                        if( _writer._spillException != null )
                        {
                            _writer._spillExceptionThrown = true;
                            throw new ChannelException("An error occurred while spilling records.", _writer._spillException);
                        }
                        else
                            throw new ObjectDisposedException(typeof(SpillRecordWriter<T>).FullName);
                    }
                    newBufferUsed = Thread.VolatileRead(ref _bufferUsed);
                    Debug.Assert(newBufferUsed >= count); // Make sure FreeBuffer doesn't free too much
                }
                _bufferPos = CopyCircular(buffer, offset, _buffer, _bufferPos, count);
            }

            public void FreeBuffer(int size)
            {
                int newSize = Interlocked.Add(ref _bufferUsed, -size);
                Debug.Assert(newSize >= 0);
                _freeBufferEvent.Set();
            }

            public void SetMark()
            {
                _bufferMark = _bufferPos;
            }

            public void UnwrapRecord()
            {
                // Thread safety note: only one thread is writing into this buffer, to _bufferUsed can only ever becomes less (because of the spill thread).
                // If that happens, we'll take an overly cautious approach but it's basically fine.
                int extraBytes = _buffer.Length - _bufferMark;
                if( _bufferUsed + extraBytes < _buffer.Length )
                {
                    System.Buffer.BlockCopy(_buffer, 0, _buffer, extraBytes, _bufferPos); // Move bytes at start of buffer forward
                    System.Buffer.BlockCopy(_buffer, _bufferMark, _buffer, 0, extraBytes); // Move bytes at the end of buffer to the start
                    _bufferPos += extraBytes;
                    int newBufferUsed = Interlocked.Add(ref _bufferUsed, extraBytes);
                    Debug.Assert(newBufferUsed < _buffer.Length);
                }
                else
                {
                    // Not enough space, so we'll use write to take care of the waiting.
                    byte[] temp = new byte[_bufferPos];
                    System.Buffer.BlockCopy(_buffer, 0, temp, 0, _bufferPos);
                    FreeBuffer(_bufferPos);
                    _bufferPos = 0;
                    Write(_buffer, _bufferMark, extraBytes);
                    Write(temp, 0, temp.Length);
                }
            }

            private static int CopyCircular(byte[] source, int sourceIndex, byte[] destination, int destinationIndex, int count)
            {
                if( source == null )
                    throw new ArgumentNullException("source");
                if( destination == null )
                    throw new ArgumentNullException("destination");
                if( sourceIndex < 0 )
                    throw new ArgumentOutOfRangeException("sourceIndex");
                if( destinationIndex < 0 )
                    throw new ArgumentOutOfRangeException("destinationIndex");
                if( count < 0 )
                    throw new ArgumentOutOfRangeException("count");
                if( sourceIndex + count > source.Length )
                    throw new ArgumentException("sourceIndex + count is larger than the source array.");
                int end = destinationIndex + count;
                if( end > destination.Length )
                {
                    end %= destination.Length;
                    if( end > destinationIndex )
                        throw new ArgumentException("count is larger than the destination array.");
                }


                if( end >= destinationIndex )
                {
                    Array.Copy(source, sourceIndex, destination, destinationIndex, count);
                }
                else
                {
                    int firstCount = destination.Length - destinationIndex;
                    Array.Copy(source, sourceIndex, destination, destinationIndex, firstCount);
                    Array.Copy(source, sourceIndex + firstCount, destination, 0, end);
                }
                return end % destination.Length;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SpillRecordWriter<>));

        private readonly IPartitioner<T> _partitioner;
        private readonly CircularBufferStream _buffer;
        private readonly BinaryWriter _bufferWriter;
        private readonly List<RecordIndexEntry>[] _indices;
        private readonly SpillRecordWriterOptions _flags;
        private int _lastPartition = -1;
        private int _bufferRemaining;
        private int _lastRecordEnd;
        private long _bytesWritten;

        private readonly RecordIndexEntry[][] _spillIndices;
        private readonly object _spillLock = new object();
        private int _spillStart;
        private int _spillEnd;
        private int _spillSize;
        private volatile bool _spillInProgress;
        private AutoResetEvent _spillWaitingEvent = new AutoResetEvent(false);
        private Thread _spillThread;
        private ManualResetEvent _cancelEvent = new ManualResetEvent(false);
        private int _spillCount;
        private Exception _spillException;
        private bool _spillExceptionThrown;
        private bool _disposed;
        private RawRecord _record;

        //private StreamWriter _debugWriter;

        /// <summary>
        /// Initializes a new instance of the <see cref="SpillRecordWriter&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="partitioner">The partitioner for the records.</param>
        /// <param name="bufferSize">The size of the in-memory buffer.</param>
        /// <param name="limit">The amount of data in the buffer when a spill is triggered.</param>
        /// <param name="options">A combination of <see cref="SpillRecordWriterOptions"/> values.</param>
        protected SpillRecordWriter(IPartitioner<T> partitioner, int bufferSize, int limit, SpillRecordWriterOptions options)
        {
            if( partitioner == null )
                throw new ArgumentNullException("partitioner");
            if( bufferSize < 0 )
                throw new ArgumentOutOfRangeException("bufferSize");
            if( limit < 1 || limit > bufferSize )
                throw new ArgumentOutOfRangeException("limit");
            _partitioner = partitioner;
            _buffer = new CircularBufferStream(this, bufferSize);
            _bufferWriter = new BinaryWriter(_buffer);
            _indices = new List<RecordIndexEntry>[partitioner.Partitions];
            _bufferRemaining = limit;
            _spillIndices = new RecordIndexEntry[partitioner.Partitions][];
            _flags = options;
            //_debugWriter = new StreamWriter(outputPath + ".debug.txt");
        }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The size of the written records after serialization.
        /// </value>
        public override long OutputBytes
        {
            get
            {
                return _bytesWritten;
            }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>The number of bytes written to the output.</value>
        public override long BytesWritten
        {
            get
            {
                return _bytesWritten;
            }
        }

        /// <summary>
        /// Gets the partitioner.
        /// </summary>
        /// <value>The partitioner.</value>
        public IPartitioner<T> Partitioner
        {
            get { return _partitioner; }
        }

        /// <summary>
        /// Gets the number of spills performed.
        /// </summary>
        public int SpillCount
        {
            get { return _spillCount; }
        }

        /// <summary>
        /// Gets a value indicating whether an error occurred during a background spill.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if an error occurred; otherwise, <see langword="false"/>.
        /// </value>
        protected bool ErrorOccurred
        {
            get { return _spillException != null; }
        }

        /// <summary>
        /// Gets the spill buffer for the current spill.
        /// </summary>
        /// <remarks>
        /// <note>
        ///   Only access this property from the <see cref="SpillOutput"/> method, and only access
        ///   those parts of the array indicates by the regions returned by <see cref="GetSpillIndex"/>
        ///   for that spill.
        /// </note>
        /// <para>
        ///   Use this if you want to to custom writing of the partitions.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        protected byte[] SpillBuffer
        {
            get
            {
                if( !_spillInProgress )
                    throw new InvalidOperationException("No spill is in progress.");
                return _buffer.Buffer;
            }
        }

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Performs the final spill, if one is needed.
        /// </para>
        /// </remarks>
        public override void FinishWriting()
        {
            if( !HasFinishedWriting )
            {
                base.FinishWriting();
                lock( _spillLock )
                {
                    while( _spillInProgress )
                        Monitor.Wait(_spillLock);

                    _cancelEvent.Set();
                }
                if( _spillThread != null )
                    _spillThread.Join();

                if( _spillException != null && !_spillExceptionThrown )
                    throw new ChannelException("An exception occurred spilling the output records.", _spillException);

                if( !_spillExceptionThrown && _buffer.BufferUsed > 0 || _spillCount == 0 )
                {
                    PrepareForSpill(true);
                    PerformSpill(true);
                }
                Debug.Assert(_spillExceptionThrown || _buffer.BufferUsed == 0);
            }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(T record)
        {
            if( _spillException != null )
            {
                _spillExceptionThrown = true;
                throw new ChannelException("An exception occurred spilling the output records.", _spillException);
            }

            if( _bufferRemaining <= 0 )
                RequestOutputSpill();

            _buffer.SetMark();

            // TODO: Make sure the entire record fits in the buffer.
            ValueWriter<T>.WriteValue(record, _bufferWriter);

            int recordStart = _buffer.BufferMark;
            int recordEnd = _buffer.BufferPos;
            _lastRecordEnd = recordEnd;

            int recordLength;
            if( recordEnd >= recordStart )
                recordLength = recordEnd - recordStart;
            else if( _flags.HasFlag(SpillRecordWriterOptions.AllowRecordWrapping) )
                recordLength = (_buffer.Size - recordStart) + recordEnd;
            else
            {
                // The record wrapped the end of the buffer, and that is not allowed by the flags
                _buffer.UnwrapRecord();
                recordStart = 0;
                recordLength = _buffer.BufferPos;
            }

            int partition = _partitioner.GetPartition(record);

            List<RecordIndexEntry> index = _indices[partition];
            if( _flags.HasFlag(SpillRecordWriterOptions.AllowMultiRecordIndexEntries) && partition == _lastPartition )
            {
                // If the new record was the same partition as the last record, we just update that one.
                int lastEntry = index.Count - 1;
                RecordIndexEntry entry = index[lastEntry];
                index[lastEntry] = new RecordIndexEntry(entry.Offset, entry.Count + recordLength);
            }
            else
            {
                // Add the new record to the relevant index.
                if( index == null )
                {
                    index = new List<RecordIndexEntry>(100);
                    _indices[partition] = index;
                }
                index.Add(new RecordIndexEntry(recordStart, recordLength));
                _lastPartition = partition;
            }

            _bufferRemaining -= recordLength;
            _bytesWritten += recordLength;
        }

        /// <summary>
        /// When overridden by a derived class, writes the spill data to the output.
        /// </summary>
        /// <param name="finalSpill">If set to <see langword="true"/>, this is the final spill.</param>
        /// <remarks>
        /// <para>
        ///   Implementers should call the <see cref="WritePartition(int,Stream)"/> method to write each spill partition to their output.
        /// </para>
        /// <para>
        ///   It is not guaranteed that the <paramref name="finalSpill"/> parameter will ever be <see langword="true"/>. If data was flushed by a background
        ///   spill and no further data was written into the buffer before <see cref="FinishWriting"/> is called, then this method is never called with
        ///   <paramref name="finalSpill"/> set to <see langword="true"/>. If you need to perform extra work after the final spill, override the
        ///   <see cref="FinishWriting"/> method instead.
        /// </para>
        /// </remarks>
        protected abstract void SpillOutput(bool finalSpill);

        /// <summary>
        /// Gets the index for the specified partition for the current spill.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <returns>The index entries.</returns>
        /// <remarks>
        /// <note>
        ///   Only call this method from the <see cref="SpillOutput"/> method.
        /// </note>
        /// </remarks>
        protected RecordIndexEntry[] GetSpillIndex(int partition)
        {
            if( !_spillInProgress )
                throw new InvalidOperationException("No spill is currently in progress.");
            return _spillIndices[partition];
        }

        /// <summary>
        /// Gets the spill data size for the specified partition.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <returns>The size of the partition's data for this spill.</returns>
        /// <remarks>
        /// <para>
        ///   Only call this method from the <see cref="SpillOutput"/> method.
        /// </para>
        /// </remarks>
        protected int SpillDataSizeForPartition(int partition)
        {
            return _spillIndices[partition] == null ? 0 : _spillIndices[partition].Sum(i => i.Count);
        }

        /// <summary>
        /// Writes the specified partition to the output.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <param name="outputStream">The output stream to write the partition to.</param>
        protected void WritePartition(int partition, Stream outputStream)
        {
            if( outputStream == null )
                throw new ArgumentNullException("outputStream");
            RecordIndexEntry[] index = _spillIndices[partition];
            if( index != null )
            {
                PreparePartition(partition, index, _buffer.Buffer);
                for( int x = 0; x < index.Length; ++x )
                {
                    if( index[x].Offset + index[x].Count > _buffer.Size )
                    {
                        int firstCount = _buffer.Size - index[x].Offset;
                        outputStream.Write(_buffer.Buffer, index[x].Offset, firstCount);
                        outputStream.Write(_buffer.Buffer, 0, index[x].Count - firstCount);
                    }
                    else
                        outputStream.Write(_buffer.Buffer, index[x].Offset, index[x].Count);
                }
            }
        }

        /// <summary>
        /// Writes the specified partition to the output using the <see cref="RawRecord"/> format, which includes record sizes.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <param name="output">The raw record writer to write the partition to.</param>
        protected void WritePartition(int partition, RecordWriter<RawRecord> output)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( _record == null )
                _record = new RawRecord();

            RawRecord record = _record;
            RecordIndexEntry[] index = _spillIndices[partition];
            if( index != null )
            {
                PreparePartition(partition, index, _buffer.Buffer);
                for( int x = 0; x < index.Length; ++x )
                {
                    record.Reset(_buffer.Buffer, index[x].Offset, index[x].Count);
                    output.WriteRecord(record);
                }
            }
        }

        /// <summary>
        /// Determines whether the current spill has data for the specified partition.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <returns>
        ///   <see langword="true" /> if the current spill has data for the specified partition; otherwise, <see langword="false" />.
        /// </returns>
        protected bool HasDataForPartition(int partition)
        {
            return _spillIndices[partition] != null;
        }

        /// <summary>
        /// When overridden in a derived class, prepares the partition for the spill.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <param name="index">The index entries for this partition.</param>
        /// <param name="buffer">The buffer containing the spill data.</param>
        /// <remarks>
        /// <note>
        ///   Do not access any part of the array other than the regions indicated in the index!
        /// </note>
        /// </remarks>
        protected virtual void PreparePartition(int partition, RecordIndexEntry[] index, byte[] buffer)
        {
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( !_disposed )
            {
                _disposed = true;

                if( disposing )
                {
                    ((IDisposable)_bufferWriter).Dispose();
                    _buffer.Dispose();
                }
            }
        }

        private void RequestOutputSpill()
        {
            lock( _spillLock )
            {
                if( !_spillInProgress )
                {
                    PrepareForSpill(false);
                    _spillWaitingEvent.Set();
                }

                if( _spillThread == null )
                {
                    _spillThread = new Thread(SpillThread) { Name = "SpillRecordWriter.SpillThread", IsBackground = true };
                    _spillThread.Start();
                }
            }
        }

        private void PrepareForSpill(bool allowEmptySpill)
        {
            bool hasRecords = false;
            for( int x = 0; x < _indices.Length; ++x )
            {
                List<RecordIndexEntry> index = _indices[x];
                if( index != null && index.Count > 0 )
                {
                    hasRecords = true;
                    _spillIndices[x] = index.ToArray();
                    index.Clear();
                }
                else
                    _spillIndices[x] = null;
            }
            if( !(hasRecords || allowEmptySpill) )
                throw new InvalidOperationException("Spill requested but nothing to spill.");

            _lastPartition = -1;
            _spillStart = _spillEnd; // _outputEnd contains the place where the last output stopped.
            _spillEnd = _lastRecordEnd; // End at the last record.
            _spillSize = _spillEnd - _spillStart;
            if( hasRecords && _spillSize <= 0 )
                _spillSize += _buffer.Size;
            _bufferRemaining += _spillSize;
            _spillInProgress = true;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void SpillThread()
        {
            try
            {
                WaitHandle[] handles = new WaitHandle[] { _spillWaitingEvent, _cancelEvent };

                while( WaitHandle.WaitAny(handles) != 1 )
                {
                    PerformSpill(false);
                }
            }
            catch( Exception ex )
            {
                _spillException = ex;
                _cancelEvent.Set(); // Make sure the writing thread doesn't get stuck waiting for buffer space to become available.
            }
        }

        private void PerformSpill(bool finalSpill)
        {
            Debug.Assert(_spillInProgress);
            try
            {
                // We don't need to take the _spillLock for the actuall spill itself, because no one is going to access the relevant variables
                // until _spillInProgress becomes false again.
                ++_spillCount;
                _log.DebugFormat("Writing output segment {0}, offset {1} to {2}.", _spillCount, _spillStart, _spillEnd);
                //lock( _debugWriter )
                //    _debugWriter.WriteLine("Starting output from {0} to {1}.", _outputStart, _outputEnd);
                SpillOutput(finalSpill);
                _log.DebugFormat("Finished writing output segment {0}.", _spillCount);
            }
            finally
            {
                lock( _spillLock )
                {
                    _spillInProgress = false;
                    Monitor.PulseAll(_spillLock);
                }
            }
            // DO NOT CALL THIS BEFORE SETTING _spillInProgress BACK TO FALSE
            // There is a race condition that allows the buffer to be filled up again
            // *before* _spillInProgress becomes false, causing the writing thread to
            // not start a new spill and then hang waiting for the buffer to free up.
            _buffer.FreeBuffer(_spillSize);
        }
    }
}
