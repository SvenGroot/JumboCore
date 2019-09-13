// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for record readers that combine the input of multiple record readers.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Record readers must inherit from <see cref="MultiInputRecordReader{T}"/>, not just implement this interface.
    /// </note>
    /// </remarks>
    public interface IMultiInputRecordReader : IRecordReader
    {
        /// <summary>
        /// Event that is raised if the value of the <see cref="CurrentPartition"/> property changes.
        /// </summary>
        event EventHandler CurrentPartitionChanged;

        /// <summary>
        /// Event raised when the value of the <see cref="CurrentPartition"/> property is about to change.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If you set <see cref="CancelEventArgs.Cancel"/> to <see langword="true"/> in the handler
        ///   for this event, the <see cref="NextPartition"/> method will skip the indicated partition
        ///   and move to the next one.
        /// </para>
        /// </remarks>
        event EventHandler<CurrentPartitionChangingEventArgs> CurrentPartitionChanging;

        /// <summary>
        /// Gets the total number of inputs readers that this record reader will have.
        /// </summary>
        int TotalInputCount { get; }

        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        int CurrentInputCount { get; }

        /// <summary>
        /// Gets a value that indicates that this record reader is allowed to reuse record instances.
        /// </summary>
        bool AllowRecordReuse { get; }

        /// <summary>
        /// Gets the buffer size to use to read input files.
        /// </summary>
        int BufferSize { get; }

        /// <summary>
        /// Gets the type of compression to use to read input files.
        /// </summary>
        CompressionType CompressionType { get; }

        /// <summary>
        /// Gets all partitions currently assigned to this reader.
        /// </summary>
        IList<int> PartitionNumbers { get; }

        /// <summary>
        /// Gets the partition that calls to <see cref="RecordReader{T}.ReadRecord"/> should return records for.
        /// </summary>
        int CurrentPartition { get; }

        /// <summary>
        /// Moves the current partition to the next partition.
        /// </summary>
        /// <returns><see langword="true"/> if the current partition was moved to the next partition; <see langword="false"/> if there were no more partitions.</returns>
        bool NextPartition();

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input.</param>
        /// <remarks>
        /// Which partitions a multi input record reader is responsible for is specified when that reader is created.
        /// All calls to <see cref="AddInput"/> must specify those exact same partitions, sorted by the partition number.
        /// </remarks>
        void AddInput(IList<RecordInput> partitions);

        /// <summary>
        /// Assigns additional partitions to this record reader.
        /// </summary>
        /// <param name="newPartitions">The new partitions to assign.</param>
        /// <remarks>
        /// <para>
        ///   New partitions may only be assigned after all inputs for the existing partitions have been received.
        /// </para>
        /// </remarks>
        void AssignAdditionalPartitions(IList<int> newPartitions);
    }
}
