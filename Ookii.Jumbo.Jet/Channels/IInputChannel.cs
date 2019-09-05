// $Id$
//
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Interface for input channels for task communication.
    /// </summary>
    public interface IInputChannel
    {
        /// <summary>
        /// Occurs when the input channel stalls waiting for space to become available in the memory storage.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If the channel consumer (e.g. a <see cref="MultiInputRecordReader{T}"/>) can free up the required amount of space,
        ///   set the <see cref="MemoryStorageFullEventArgs.CancelWaiting"/> property to <see langword="false"/> so the memory
        ///   storage manager will continue waiting for the request.
        /// </para>
        /// <para>
        ///   If the <see cref="MemoryStorageFullEventArgs.CancelWaiting"/> property is left at its default value of <see langword="true"/>,
        ///   the memory storage manager will immediately deny the request so the channel will store the input on disk instead.
        /// </para>
        /// </remarks>
        event EventHandler<MemoryStorageFullEventArgs> MemoryStorageFull;
        
        /// <summary>
        /// Gets the configuration of the input channel.
        /// </summary>
        /// <value>The configuration of the input channel.</value>
        ChannelConfiguration Configuration { get; }

        /// <summary>
        /// Gets the input stage of this channel.
        /// </summary>
        /// <value>The <see cref="StageConfiguration"/> of the input stage.</value>
        StageConfiguration InputStage { get; }

        /// <summary>
        /// Gets a value indicating whether the input channel uses memory storage to store inputs.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
        /// </value>
        bool UsesMemoryStorage { get; }

        /// <summary>
        /// Gets the current memory storage usage level.
        /// </summary>
        /// <value>The memory storage usage level, between 0 and 1.</value>
        /// <remarks>
        /// <para>
        ///   The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
        /// </para>
        /// <para>
        ///   If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
        ///   the actual level.
        /// </para>
        /// </remarks>
        float MemoryStorageLevel { get; }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        /// <remarks>
        /// If the task has more than one input, the record reader will combine all inputs, usually by serializing them.
        /// </remarks>
        IRecordReader CreateRecordReader();

        /// <summary>
        /// Assigns additional partitions to this input channel.
        /// </summary>
        /// <param name="additionalPartitions">The additional partitions.</param>
        /// <remarks>
        /// <para>
        ///   Not all input channels need to support this.
        /// </para>
        /// <para>
        ///   This method will only be called after the task finished processing all previously assigned partitions.
        /// </para>
        /// <para>
        ///   This method will never be called if <see cref="ChannelConfiguration.PartitionsPerTask"/> is 1
        ///   or <see cref="ChannelConfiguration.DisableDynamicPartitionAssignment"/> is <see langword="true"/>.
        /// </para>
        /// </remarks>
        void AssignAdditionalPartitions(IList<int> additionalPartitions);
    }
}
