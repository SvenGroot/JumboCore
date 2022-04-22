// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Non-generic interface for record writers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record writers must inherit from <see cref="RecordWriter{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IRecordWriter : IDisposable
    {
        /// <summary>
        /// Gets the total number of records written by this record writer.
        /// </summary>
        int RecordsWritten { get; }

        /// <summary>
        /// Gets the size of the written records after serialization.
        /// </summary>
        /// <value>
        /// The size of the written records after serialization, or 0 if this writer did not serialize the records.
        /// </value>
        long OutputBytes { get; }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>
        /// The number of bytes written to the output.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This is the value of <see cref="OutputBytes"/>, adjusted for compression (if applicable) and including any additional data written by the record writer (if any).
        /// </para>
        /// </remarks>
        long BytesWritten { get; }

        /// <summary>
        /// Gets the time spent writing.
        /// </summary>
        /// <value>
        /// The time spent writing.
        /// </value>
        TimeSpan WriteTime { get; }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        void WriteRecord(object record);

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        /// <remarks>
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
        ///   The <see cref="IDisposable.Dispose"/> implementation for <see cref="RecordWriter{T}"/> will call this method.
        /// </para>
        /// </remarks>
        void FinishWriting();
    }
}
