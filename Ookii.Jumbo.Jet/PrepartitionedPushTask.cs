using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Base class for tasks that use the push model and can receive records of multiple partitions.
    /// </summary>
    /// <typeparam name="TInput">The type of the input.</typeparam>
    /// <typeparam name="TOutput">The type of the output.</typeparam>
    /// <remarks>
    /// <para>
    ///   This task type is meant for use with receiving tasks of a pipeline channel with internal partitioning. It prevents the overhead of creating a <see cref="TaskExecutionUtility"/>
    ///   and task instance for every partition.
    /// </para>
    /// <para>
    ///   If the task needs to know how many partitions there are it should check the <see cref="Jobs.StageConfiguration.InternalPartitionCount"/> property
    ///   of the <see cref="TaskContext.StageConfiguration"/> property.
    /// </para>
    /// <para>
    ///   Although tasks using this interface are free to change the partition a record belongs to, it cannot change the number of partitions.
    ///   All output partition numbers must be between 0 inclusive and <see cref="Jobs.StageConfiguration.InternalPartitionCount"/> exclusive.
    /// </para>
    /// </remarks>
    public abstract class PrepartitionedPushTask<TInput, TOutput> : Configurable, ITask<TInput, TOutput>
    {
        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="partition">The partition of the record.</param>
        /// <param name="output">The <see cref="PrepartitionedRecordWriter{T}"/> to which the task's output should be written.</param>
        public abstract void ProcessRecord(TInput record, int partition, PrepartitionedRecordWriter<TOutput> output);

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="PrepartitionedRecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        public virtual void Finish(PrepartitionedRecordWriter<TOutput> output)
        {
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// <para>
        ///   While it is possible to override this method, it is not guaranteed to be called in all scenarios (e.g. if this
        ///   task is receiving data from a pipeline channel). The task must function correctly even if the <see cref="ProcessRecord"/>
        ///   and <see cref="Finish"/> are called directly.
        /// </para>
        /// </remarks>
        public virtual void Run(RecordReader<TInput> input, RecordWriter<TOutput> output)
        {
            ArgumentNullException.ThrowIfNull(input);
            // Safe to use using because PrepartitionedRecordWriter does not dispose the base stream.
            using (var prepartitionedOutputWriter = new PrepartitionedRecordWriter<TOutput>(output, false))
            {
                foreach (var record in input.EnumerateRecords())
                {
                    ProcessRecord(record, 0, prepartitionedOutputWriter);
                }
                Finish(prepartitionedOutputWriter);
            }
        }
    }
}
