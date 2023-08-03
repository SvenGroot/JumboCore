// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Record reader used for pull tasks with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   A pull task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute may try to cast its input record reader
    ///   to this type to retrieve information about the number of partitions and the current partition.
    /// </para>
    /// <para>
    ///   However, if the input to a pull task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute is not
    ///   a channel with multiple partitions per task, the input record reader will not be a <see cref="MultiPartitionRecordReader{T}"/>
    ///   so you should not assume that such a cast will always succeed.
    /// </para>
    /// </remarks>
    public sealed class MultiPartitionRecordReader<T> : RecordReader<T>
        where T : notnull
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MultiPartitionRecordReader<T>));

        private readonly TaskExecutionUtility _taskExecution;
        private readonly MultiInputRecordReader<T> _baseReader; // Do not override Dispose to dispose of the _baseReader. TaskExecutionUtility will need it later.

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiPartitionRecordReader&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for this task. May be <see langword="null"/>.</param>
        /// <param name="baseReader">The <see cref="MultiInputRecordReader{T}"/> to read from.</param>
        public MultiPartitionRecordReader(TaskExecutionUtility taskExecution, MultiInputRecordReader<T> baseReader)
            : base(false)
        {
            ArgumentNullException.ThrowIfNull(baseReader);

            _taskExecution = taskExecution;
            _baseReader = baseReader;
            _baseReader.CurrentPartitionChanging += new EventHandler<CurrentPartitionChangingEventArgs>(_baseReader_CurrentPartitionChanging);
            _baseReader.HasRecordsChanged += new EventHandler(_baseReader_HasRecordsChanged);
            _log.InfoFormat("Now processing partition {0}.", _baseReader.CurrentPartition);
            AllowAdditionalPartitions = true;
            HasRecords = baseReader.HasRecords;
        }

        /// <summary>
        /// Gets a number between 0 and 1 that indicates the progress of the reader.
        /// </summary>
        /// <value>The progress.</value>
        public override float Progress
        {
            get { return _baseReader.Progress; }
        }

        /// <summary>
        /// Gets the partition of the current record.
        /// </summary>
        /// <value>The current partition.</value>
        public int CurrentPartition
        {
            get { return _baseReader.CurrentPartition; }
        }

        /// <summary>
        /// Gets the total number of partitions.
        /// </summary>
        /// <value>The total number of partitions.</value>
        public int PartitionCount
        {
            get { return _baseReader.PartitionCount; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the task may request additional partitions from the job server if it finishes the current ones.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if getting additional partitions is allowed; otherwise, <see langword="false"/>. The default value is <see langword="true"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   A task can set this property to <see langword="false"/> to prevent the task from requesting additional partitions.
        /// </para>
        /// <para>
        ///   If this property is <see langword="false"/>, and a call to <see cref="RecordReader{T}.ReadRecord"/> returned <see langword="false"/>, you
        ///   may change this property to <see langword="true"/> and attempt the call to <see cref="RecordReader{T}.ReadRecord"/> again.
        /// </para>
        /// <para>
        ///   Note that if this stage doesn't use multiple partitions per task or if dynamic partition assignment was disabled a
        ///   task will never get additional partitions even if this property is <see langword="true"/>.
        /// </para>
        /// </remarks>
        public bool AllowAdditionalPartitions { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether reading records will halt at the end of the current partition.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if reading records will halt at the end of the current partition; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this property is <see langword="false" />, the <see cref="RecordReader{T}.ReadRecord"/> function will return false when the
        ///   end of the current partition is reached.
        /// </para>
        /// <para>
        ///   Like setting <see cref="AllowAdditionalPartitions"/> to <see langword="false"/>, this will also prevent additional partitions from being fetched.
        /// </para>
        /// <para>
        ///   To advance to the next partition, set this property back to <see langword="false"/>, and call <see cref="RecordReader{T}.ReadRecord"/> again.
        /// </para>
        /// </remarks>
        public bool StopAtEndOfPartition { get; set; }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read; <see langword="false"/> if there are no more records.</returns>
        protected override bool ReadRecordInternal()
        {
            while (!_baseReader.ReadRecord())
            {
                if (!NextPartition())
                {
                    CurrentRecord = default(T);
                    return false;
                }
            }

            CurrentRecord = _baseReader.CurrentRecord;
            return true;
        }

        private bool NextPartition()
        {
            // If .NextPartition fails we will check for additional partitions, and if we got any, we need to call NextPartition again.
            if (StopAtEndOfPartition || !(_baseReader.NextPartition() || (AllowAdditionalPartitions && _taskExecution != null && _taskExecution.GetAdditionalPartitions(_baseReader) && _baseReader.NextPartition())))
                return false;

            _log.InfoFormat("Now processing partition {0}.", _baseReader.CurrentPartition);
            return true;
        }

        private void _baseReader_CurrentPartitionChanging(object? sender, CurrentPartitionChangingEventArgs e)
        {
            if (_taskExecution != null)
                e.Cancel = !_taskExecution.NotifyStartPartitionProcessing(e.NewPartitionNumber);
        }

        private void _baseReader_HasRecordsChanged(object? sender, EventArgs e)
        {
            HasRecords = _baseReader.HasRecords;
        }
    }
}
