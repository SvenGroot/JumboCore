// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the the channel between two pipelined tasks.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Pipelined tasks are run in the same process, and each call to <see cref="RecordWriter{T}.WriteRecord"/> will invoke
    ///   the associated task's <see cref="PushTask{TInput,TOutput}.ProcessRecord"/> method. Because of this, there is no
    ///   associated input channel for this channel type.
    /// </para>
    /// </remarks>
    public sealed class PipelineOutputChannel : IOutputChannel
    {
        private readonly TaskExecutionUtility _taskExecution;

        /// <summary>
        /// Initializes a new instance of the <see cref="PipelineOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public PipelineOutputChannel(TaskExecutionUtility taskExecution)
        {
            if (taskExecution == null)
                throw new ArgumentNullException(nameof(taskExecution));

            _taskExecution = taskExecution;
        }

        #region IOutputChannel Members

        /// <summary>
        /// Creates a record writer for the channel.
        /// </summary>
        /// <typeparam name="T">The type of record.</typeparam>
        /// <returns>A record writer for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public RecordWriter<T> CreateRecordWriter<T>()
        {
            var childStage = _taskExecution.Context.StageConfiguration.ChildStage;
            IPartitioner<T> partitioner;
            var taskCount = childStage.TaskCount;

            if (_taskExecution.Context.StageConfiguration.IsOutputPrepartitioned && _taskExecution.Context.StageConfiguration.InternalPartitionCount != 1)
            {
                // If the parent stage has multiple internal partitions and uses pre-partitioned output, but the next childstage doesn't use IPrepartitionedPushTask
                // we need to split here to match the number of internal partitions.
                taskCount = _taskExecution.Context.StageConfiguration.InternalPartitionCount;
            }

            if (childStage.IsOutputPrepartitioned)
            {
                partitioner = (IPartitioner<T>)JetActivator.CreateInstance(_taskExecution.Context.StageConfiguration.ChildStagePartitionerType.ReferencedType, _taskExecution);
                return (RecordWriter<T>)_taskExecution.CreateAssociatedTask(childStage, 1).CreatePipelineRecordWriter(partitioner);
            }
            else if (taskCount == 1)
                return (RecordWriter<T>)_taskExecution.CreateAssociatedTask(childStage, 1).CreatePipelineRecordWriter(null);
            else
            {
                var writers = new List<RecordWriter<T>>();
                partitioner = (IPartitioner<T>)JetActivator.CreateInstance(_taskExecution.Context.StageConfiguration.ChildStagePartitionerType.ReferencedType, _taskExecution);

                for (var x = 1; x <= taskCount; ++x)
                {
                    var childTaskExecution = _taskExecution.CreateAssociatedTask(childStage, x);
                    writers.Add((RecordWriter<T>)childTaskExecution.CreatePipelineRecordWriter(null));
                }
                return new MultiRecordWriter<T>(writers, partitioner);
            }
        }

        #endregion

    }
}
