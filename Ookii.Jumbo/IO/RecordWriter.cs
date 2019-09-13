// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Ookii.Jumbo.IO;
using System.Globalization;
using System.Diagnostics;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides methods for inspecting record writers.
    /// </summary>
    public static class RecordWriter
    {
        /// <summary>
        /// Gets the type of the records for the specified record writer.
        /// </summary>
        /// <param name="recordWriterType">The type of the record writer.</param>
        /// <returns>The record type</returns>
        public static Type GetRecordType(Type recordWriterType)
        {
            if( recordWriterType == null )
                throw new ArgumentNullException("recordWriterType");
            Type baseType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true);
            return baseType.GetGenericArguments()[0];
        }
    }
    /// <summary>
    /// Abstract base class for classes that write records.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    /// <remarks>
    /// <para>
    ///   All records passed to <see cref="RecordWriter{T}.WriteRecord"/> must be <typeparamref name="T"/>; they may not be a type derived
    ///   from <typeparamref name="T"/>.
    /// </para>
    /// </remarks>
    public abstract class RecordWriter<T> : IRecordWriter, IDisposable
    {
        private int _recordsWritten;
        private bool _finishedWriting;
        private readonly bool _recordTypeIsSealed = typeof(T).IsSealed;
        private readonly Stopwatch _writeTime = new Stopwatch();

        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        public int RecordsWritten
        {
            get { return _recordsWritten; }
        }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The size of the written records after serialization, or 0 if this writer did not serialize the records.
        /// </value>
        public virtual long OutputBytes 
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>The number of bytes written to the output.</value>
        /// <remarks>
        /// This is the value of <see cref="OutputBytes"/>, adjusted for compression (if applicable) and including any additional data written by the record writer (if any).
        /// If this property is not overridden, the value of <see cref="OutputBytes"/> is returned.
        /// </remarks>
        public virtual long BytesWritten
        {
            get { return OutputBytes; }
        }

        /// <summary>
        /// Gets a value indicating whether <see cref="FinishWriting"/> method has been called.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has finished writing; otherwise, <see langword="false"/>.
        /// </value>
        public bool HasFinishedWriting
        {
            get { return _finishedWriting; }
        }

        /// <summary>
        /// Gets the time spent writing.
        /// </summary>
        /// <value>
        /// The time spent writing.
        /// </value>
        public TimeSpan WriteTime
        {
            get { return _writeTime.Elapsed; }
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        public void WriteRecord(T record)
        {
            _writeTime.Start();
            try
            {
                if( record == null )
                    throw new ArgumentNullException("record");
                // Skip the type check if the record type is sealed.
                if( !_recordTypeIsSealed && record.GetType() != typeof(T) )
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "The record was type {0} rather than {1}.", record.GetType(), typeof(T)), "record");
                if( _finishedWriting )
                    throw new InvalidOperationException("Cannot write additional records after the FinishWriting method has been called.");
                WriteRecordInternal(record);
                // Increment this after the write, so if the implementation of WriteRecordsInternal throws an exception the count
                // is not incremented.
                ++_recordsWritten;
            }
            finally
            {
                _writeTime.Stop();
            }
        }

        /// <summary>
        /// Writes the specified sequence of records.
        /// </summary>
        /// <param name="records">The records to write.</param>
        /// <remarks>
        /// <para>
        ///   This is primarily a helper function so that you can easily write the result of a LINQ expression
        ///   to a record writer.
        /// </para>
        /// </remarks>
        public void WriteRecords(IEnumerable<T> records)
        {
            if( records == null )
                throw new ArgumentNullException("records");

            foreach( T record in records )
                WriteRecord(record);
        }

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        /// <remarks>
        /// <note>
        ///   Overriders must call the base class implementation to ensure the <see cref="HasFinishedWriting"/> property gets set.
        /// </note>
        /// <para>
        ///   This method is intended for record writers that need to perform additional writing to finalize their output. In Jumbo Jet, this
        ///   method will be called before the writer's metrics are collected so they can include these additional writes.
        /// </para>
        /// <para>
        ///   It is allowed to dispose any output streams or other objects related to the output when this method is called, as no more
        ///   writes will occur after that point. However, the <see cref="RecordsWritten"/> <see cref="BytesWritten"/> and <see cref="OutputBytes"/>
        ///   properties must still return the correct values after <see cref="FinishWriting"/> has been called.
        /// </para>
        /// <para>
        ///   The <see cref="IDisposable.Dispose"/> implementation for <see cref="RecordWriter{T}"/> will call this method. It is recommended
        ///   to make it safe to call this method multiple times.
        /// </para>
        /// </remarks>
        public virtual void FinishWriting()
        {
            _finishedWriting = true;
        }

        /// <summary>
        /// When implemented in a derived class, writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected abstract void WriteRecordInternal(T record);

        /// <summary>
        /// Cleans up all resources associated with this <see cref="RecordWriter{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( !_finishedWriting )
                FinishWriting();
        }

        void IRecordWriter.WriteRecord(object record)
        {
            WriteRecord((T)record);
        }

        #region IDisposable Members

        /// <summary>
        /// Cleans up all resources held by this <see langword="RecordWriter{T}"/>.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

    }
}
