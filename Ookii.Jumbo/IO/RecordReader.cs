// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.Diagnostics;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides methods for inspecting record readers.
    /// </summary>
    public static class RecordReader
    {
        /// <summary>
        /// Gets the type of the records for the specified record reader.
        /// </summary>
        /// <param name="recordReaderType">The type of the record reader.</param>
        /// <returns>The record type</returns>
        public static Type GetRecordType(Type recordReaderType)
        {
            if( recordReaderType == null )
                throw new ArgumentNullException("recordReaderType");
            Type baseType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true);
            return baseType.GetGenericArguments()[0];
        }
    }

    /// <summary>
    /// Abstract base class for record readers.
    /// </summary>
    /// <typeparam name="T">The type of the record</typeparam>
    public abstract class RecordReader<T> : IRecordReader, IDisposable
    {
        private readonly Stopwatch _readTime = new Stopwatch();
        private int _recordsRead;
        private bool _hasRecords;
        private bool _hasFinished;

        /// <summary>
        /// Occurs when the value of the <see cref="HasRecords"/> property changes.
        /// </summary>
        public event EventHandler HasRecordsChanged;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class.
        /// </summary>
        protected RecordReader()
            : this(true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordReader{T}"/> class.
        /// </summary>
        /// <param name="initialHasRecords">The initial value of the <see cref="HasRecords"/> property.</param>
        protected RecordReader(bool initialHasRecords)
        {
            _hasRecords = initialHasRecords;
        }

        /// <summary>
        /// Gets or sets the an informational string indicating the source of the records.
        /// </summary>
        /// <remarks>
        /// This property is used for record readers passed to merge tasks in Jumbo Jet to indicate
        /// the task that this reader's data originates from.
        /// </remarks>
        public string SourceName { get; set; }

        /// <summary>
        /// Gets the number of records that has been read by this record reader.
        /// </summary>
        public int RecordsRead
        {
            get { return _recordsRead; }
        }

        /// <summary>
        /// Gets a number between 0 and 1 that indicates the progress of the reader.
        /// </summary>
        public abstract float Progress { get; }

        /// <summary>
        /// Gets the size of the records before deserialization.
        /// </summary>
        /// <value>
        /// The size of the records before deserialization, or 0 if the records were not read from a serialized source.
        /// </value>
        public virtual long InputBytes 
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the actual number of bytes read from the input.
        /// </summary>
        /// <value>The number of bytes read from the input.</value>
        /// <remarks>
        /// This is the value of <see cref="InputBytes"/>, adjusted for compression (if applicable) and including any additional data read by the record reader (if any).
        /// </remarks>
        public virtual long BytesRead
        {
            get { return InputBytes; }
        }

        /// <summary>
        /// Gets the current record.
        /// </summary>
        public T CurrentRecord { get; protected set; }

        /// <summary>
        /// Gets a value that indicates whether there are records available on the data source that this reader is reading from.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has records available and is not waiting for input; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   The <see cref="HasRecords"/> property indicates if the record reader is waiting for an external source to provide it
        ///   with data, or has data available from which it can read records immediately. If this property
        ///   is <see langword="true"/>, it indicates that the <see cref="ReadRecord"/> method will not
        ///   block waiting for an external event (it may, however, still block waiting for IO).
        /// </para>
        /// <para>
        ///   For example, a multi-input record reader may use the <see cref="HasRecords"/> property to indicate whether any inputs
        ///   have been added yet. If this multi-input record reader is reading from a file channel, this could
        ///   be used to determine if the reader is waiting for data to be shuffled or if it is available now.
        /// </para>
        /// <para>
        ///   If the <see cref="HasRecords"/> property is <see langword="false"/>, it is still safe to call <see cref="ReadRecord"/>,
        ///   there is just no guarantee that the call will return immediately.
        /// </para>
        /// <para>
        ///   If the <see cref="HasRecords"/> property is <see langword="false"/> and <see cref="HasFinished"/> is <see langword="false"/>,
        ///   then the <see cref="HasRecords"/> property must become <see langword="true"/> at some point, provided there are no error
        ///   conditions.
        /// </para>
        /// <para>
        ///   If the <see cref="HasRecords"/> property is <see langword="true"/>, the next call to <see cref="ReadRecord"/> can
        ///   still return <see langword="false"/>. After <see cref="ReadRecord"/> has returned <see langword="false"/>, the
        ///   <see cref="HasRecords"/> property will also be <see langword="false"/>
        /// </para>
        /// <para>
        ///   When the <see cref="HasRecords"/> property changes, the <see cref="HasRecordsChanged"/> event will be raised.
        /// </para>
        /// <para>
        ///   For multi-input record readers, this property applies only to the current partition; if the current partition
        ///   changes, the value of the <see cref="HasRecords"/> property should be reset.
        /// </para>
        /// <para>
        ///   This is a default implementation for <see cref="IRecordReader.HasRecords"/> that simply always returns <see langword="true"/> until
        ///   a call to <see cref="ReadRecord"/> has returned <see langword="false"/>.
        /// </para>
        /// </remarks>
        public bool HasRecords
        {
            get { return !_hasFinished && _hasRecords; }
            protected set
            {
                if( _hasRecords != value )
                {
                    _hasRecords = value;
                    OnHasRecordsChanged(EventArgs.Empty);
                }
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has read all records.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has finished; otherwise, <see langword="false"/>.
        /// </value>
        public bool HasFinished
        {
            get { return _hasFinished; }  // _hasRecords caches the result of the last ReadRecordInternal call, so we can use it for this.
        }

        /// <summary>
        /// Gets the time spent reading.
        /// </summary>
        /// <value>
        /// The time spent reading.
        /// </value>
        public TimeSpan ReadTime
        {
            get { return _readTime.Elapsed; }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read; <see langword="false"/> if there are no more records.</returns>
        public bool ReadRecord()
        {
            _readTime.Start();
            try
            {
                if( ReadRecordInternal() )
                {
                    if( _hasFinished ) // Can happen with record readers that process multiple partitions.
                    {
                        _hasFinished = false;
                        OnHasRecordsChanged(EventArgs.Empty);
                    }
                    ++_recordsRead;
                    return true;
                }
                else
                {
                    if( !_hasFinished )
                    {
                        _hasFinished = true;
                        OnHasRecordsChanged(EventArgs.Empty);
                    }
                    return false;
                }
            }
            finally
            {
                _readTime.Stop();
            }
        }

        /// <summary>
        /// Enumerates over all the records.
        /// </summary>
        /// <returns>An implementation of <see cref="IEnumerable{T}"/> that enumerates over the records.</returns>
        public IEnumerable<T> EnumerateRecords()
        {
            while( ReadRecord() )
            {
                yield return CurrentRecord;
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read; <see langword="false"/> if there are no more records.</returns>
        protected abstract bool ReadRecordInternal();

        /// <summary>
        /// Raises the <see cref="HasRecordsChanged"/> event.
        /// </summary>
        /// <param name="e">The <see cref="System.EventArgs"/> instance containing the event data.</param>
        protected virtual void OnHasRecordsChanged(EventArgs e)
        {
            EventHandler handler = HasRecordsChanged;
            if( handler != null )
                handler(this, e);
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all resources held by this <see langword="StreamRecordReader{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Explicit IRecordReader Members

        object IRecordReader.CurrentRecord
        {
            get { return CurrentRecord; }
        }

        #endregion
    }
}
