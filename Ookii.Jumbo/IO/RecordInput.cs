// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Represents an input to the <see cref="MultiInputRecordReader{T}"/> class.
    /// </summary>
    public abstract class RecordInput : IDisposable
    {
        private IRecordReader _reader;
        private RecordReader<RawRecord> _rawReader;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class.
        /// </summary>
        protected RecordInput()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordInput"/> class.
        /// </summary>
        /// <param name="reader">The reader for this input.</param>
        protected RecordInput(IRecordReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException("reader");
            _reader = reader;
        }

        /// <summary>
        /// Gets a value indicating whether this input is read from memory.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this input is read from memory; <see langword="false"/> if it is read from a file.
        /// </value>
        public abstract bool IsMemoryBased { get; }

        /// <summary>
        /// Gets a value indicating whether this instance supports the raw record reader.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance supports the raw record reader; otherwise, <see langword="false"/>.
        /// </value>
        public abstract bool IsRawReaderSupported { get; }

        /// <summary>
        /// Gets the record reader for this input.
        /// </summary>
        /// <value>The record reader.</value>
        /// <remarks>
        /// <para>
        ///   If the reader had not yet been created, it will be created by accessing this property.
        /// </para>
        /// </remarks>
        public IRecordReader Reader
        {
            get
            {
                // TODO: Should make this a method because it has negative side-effects, which means the debugger shouldn't evaluate it.
                CheckDisposed();
                if( _rawReader != null )
                    throw new InvalidOperationException("This input already has a raw record reader.");
                if( _reader == null )
                    _reader = CreateReader();
                return _reader;
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has records available.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the <see cref="RecordReader{T}.HasRecords"/> property is <see langword="true"/>; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This property can be accessed without creating the record reader if it had not yet been created.
        /// </para>
        /// </remarks>
        public virtual bool HasRecords
        {
            get
            {
                // We treat inputs whose reader hasn't yet been created as if RecordsAvailable is true, as they are normally read from a file
                // so their readers would always return true anyway.
                return _reader == null || _reader.HasRecords;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the record reader has been created.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the record reader has been created; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this value is <see langword="false"/>, it means that the <see cref="Reader"/> property will
        ///   create a file-based record reader when accessed, which is guaranteed never to return <see langword="false"/>
        ///   for the <see cref="RecordReader{T}.HasRecords"/> property.
        /// </para>
        /// </remarks>
        public bool IsReaderCreated
        {
            get
            {
                return _reader != null;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the record reader has been created.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the raw record reader has been created; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this value is <see langword="false"/>, it means that the <see cref="GetRawReader"/> method will
        ///   create a file-based record reader when accessed, which is guaranteed never to return <see langword="false"/>
        ///   for the <see cref="RecordReader{T}.HasRecords"/> property.
        /// </para>
        /// </remarks>
        public bool IsRawReaderCreated
        {
            get
            {
                return _rawReader != null;
            }
        }

        internal float Progress
        {
            get
            {
                if( _disposed )
                    return 1.0f;
                else if( IsReaderCreated )
                    return _reader.Progress;
                else if( IsRawReaderCreated )
                    return _rawReader.Progress;
                else
                    return 0.0f;
            }
        }

        /// <summary>
        /// Releases all resources used by this RecordInput.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Gets the raw record reader for this input.
        /// </summary>
        /// <returns>The raw record reader.</returns>
        /// <remarks>
        /// <para>
        ///   If the reader had not yet been created, it will be created by this method.
        /// </para>
        /// <para>
        ///   The raw reader requires that the records were written using a <see cref="BinaryRecordWriter{T}"/> with <see cref="RawRecord"/>
        ///   as the record type.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Code has non-safe side-effects")]
        public RecordReader<RawRecord> GetRawReader()
        {
            CheckDisposed();
            if( _reader != null )
                throw new InvalidOperationException("This input already has a regular record reader.");
            if( _rawReader == null )
                _rawReader = CreateRawReader();
            return _rawReader;
        }

        /// <summary>
        /// Creates the record reader for this input.
        /// </summary>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected abstract IRecordReader CreateReader();

        /// <summary>
        /// Creates the raw record reader for this input.
        /// </summary>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected abstract RecordReader<RawRecord> CreateRawReader();

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_disposed )
            {
                _disposed = true;
                if( _reader != null )
                {
                    ((IDisposable)_reader).Dispose();
                }
                if( _rawReader != null )
                {
                    _rawReader.Dispose();
                }
            }
        }

        private void CheckDisposed()
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(RecordInput).Name);
        }
    }
}
