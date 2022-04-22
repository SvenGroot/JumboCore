// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Threading;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Performs an in-memory sort of its input records. The sorting algorithm used is QuickSort.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <remarks>
    /// <note>
    ///   The class that generates the input for this task (which can be either another task if a pipeline channel is used, or a <see cref="RecordReader{T}"/>)
    ///   may not reuse the record instances for the records.
    /// </note>
    /// <note>
    ///   This task performs an in-memory sort of all records. Use it to sort small amounts of records only. For large (or unknown) numbers of records, use the file channel with <see cref="Ookii.Jumbo.Jet.Channels.FileChannelOutputType.SortSpill"/>
    ///   (e.g. using the <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder.SpillSortCombine"/> function).
    /// </note>
    /// </remarks>
    public class SortTask<T> : PrepartitionedPushTask<T, T>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SortTask<T>));
        private List<T>[] _partitions;
        private IComparer<T> _comparer;

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            _comparer = null;
            if (TaskContext != null)
            {
                var comparerTypeName = TaskContext.StageConfiguration.GetSetting(TaskConstants.SortTaskComparerSettingKey, null);
                if (!string.IsNullOrEmpty(comparerTypeName))
                    _comparer = (IComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskContext);
                _partitions = new List<T>[TaskContext.StageConfiguration.InternalPartitionCount];
            }
            else
                _partitions = new List<T>[1];

            for (var x = 0; x < _partitions.Length; ++x)
                _partitions[x] = new List<T>();

            if (_comparer == null)
                _comparer = Comparer<T>.Default;
        }

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="partition">The partition of the record</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public override void ProcessRecord(T record, int partition, PrepartitionedRecordWriter<T> output)
        {
            _partitions[partition].Add(record);
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public override void Finish(PrepartitionedRecordWriter<T> output)
        {
            ArgumentNullException.ThrowIfNull(output);

            var parallelSort = TaskContext == null ? true : TaskContext.GetSetting(TaskConstants.SortTaskUseParallelSortSettingKey, true);

            // Don't do parallel sort if we've been told not do, or if it doesn't make sense (1 partition or 1 CPU).
            if (parallelSort && _partitions.Length > 1 && Environment.ProcessorCount > 1)
            {
                SortAndOutputPartitionsParallel(output);
            }
            else
            {
                SortAndOutputPartitionsNonParallel(output);
            }
        }

        private void SortAndOutputPartitionsNonParallel(PrepartitionedRecordWriter<T> output)
        {
            _log.DebugFormat("Sorting {0} partitions using non-parallel sort.", _partitions.Length);
            for (var partition = 0; partition < _partitions.Length; ++partition)
            {
                var records = _partitions[partition];
                records.Sort(_comparer);
                _log.DebugFormat("Done sorting partition {0}.", partition);
                foreach (var record in records)
                {
                    output.WriteRecord(record, partition);
                }
                _log.DebugFormat("Done writing partition {0}.", partition);
            }
            _log.Debug("Done sorting.");
        }

        private void SortAndOutputPartitionsParallel(PrepartitionedRecordWriter<T> output)
        {
            _log.DebugFormat("Sorting {0} partitions using parallel sort.", _partitions.Length);

            using (var evt = new CountdownEvent(_partitions.Length))
            {
                foreach (var partition in _partitions)
                {
                    var localPartition = partition; // Don't use iteration variable directly in lambda.
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        localPartition.Sort(_comparer);
                        evt.Signal();
                    });
                }
                evt.Wait();
            }

            // Writing records is not done as part of the parallel sort because the recordwriter doesn't need to be thread safe.
            _log.Debug("Done sorting.");
            for (var partition = 0; partition < _partitions.Length; ++partition)
            {
                foreach (var record in _partitions[partition])
                    output.WriteRecord(record, partition);
            }
            _log.Debug("Done writing partitions.");
        }
    }
}
