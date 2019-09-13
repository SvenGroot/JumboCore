// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Non-generic interface for record readers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record readers must inherit from <see cref="RecordReader{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IRecordReader : IDisposable
    {
        /// <summary>
        /// Occurs when the value of the <see cref="HasRecords"/> property changes.
        /// </summary>
        event EventHandler HasRecordsChanged;

        /// <summary>
        /// Gets the number of records that has been read by this record reader.
        /// </summary>
        int RecordsRead { get; }

        /// <summary>
        /// Gets the size of the records before deserialization.
        /// </summary>
        /// <value>
        /// The size of the records before deserialization, or 0 if the records were not read from a serialized source.
        /// </value>
        long InputBytes { get; }

        /// <summary>
        /// Gets the actual number of bytes read from the input.
        /// </summary>
        /// <value>
        /// The number of bytes read from the input.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This is the value of <see cref="InputBytes"/>, adjusted for compression (if applicable) and including any additional data read by the record reader (if any).
        /// </para>
        /// </remarks>
        long BytesRead { get; }

        /// <summary>
        /// Gets the progress for the task, between 0 and 1.
        /// </summary>
        float Progress { get; }

        /// <summary>
        /// Gets the current record.
        /// </summary>
        object CurrentRecord { get; }

        /// <summary>
        /// Gets or sets the an informational string indicating the source of the records.
        /// </summary>
        string SourceName { get; set;  }

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
        /// </remarks>
        bool HasRecords { get; }

        /// <summary>
        /// Gets a value indicating whether this instance has read all records.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance has finished; otherwise, <see langword="false"/>.
        /// </value>
        bool HasFinished { get; }

        /// <summary>
        /// Gets the time spent reading.
        /// </summary>
        /// <value>
        /// The time spent reading.
        /// </value>
        TimeSpan ReadTime { get; }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        bool ReadRecord();
    }
}
