// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Provides base functionality for <see cref="IOutputChannel"/> implementations.
    /// </summary>
    public abstract class OutputChannel : IOutputChannel
    {
        /// <summary>
        /// The name of the setting in <see cref="Jobs.JobConfiguration.JobSettings"/> or <see cref="Jobs.StageConfiguration.StageSettings"/> that overrides the global compression setting.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
        public const string CompressionTypeSetting = "FileChannel.CompressionType";

        private readonly List<string> _outputPartitionIds = new List<string>();
        private ReadOnlyCollection<string> _outputIdsReadOnlyWrapper;

        /// <summary>
        /// Initializes a new instance of the <see cref="OutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        protected OutputChannel(TaskExecutionUtility taskExecution)
        {
            if( taskExecution == null )
                throw new ArgumentNullException(nameof(taskExecution));

            TaskExecution = taskExecution;

            ChannelConfiguration channelConfig = taskExecution.Context.StageConfiguration.OutputChannel;
            if( channelConfig.OutputStage != null )
            {
                StageConfiguration outputStage = taskExecution.Context.JobConfiguration.GetStage(channelConfig.OutputStage);
                if( taskExecution.Context.StageConfiguration.InternalPartitionCount == 1 || taskExecution.Context.StageConfiguration.IsOutputPrepartitioned )
                {
                    // If this task is not a child of a compound task, or there is no partitioning done inside the compound,
                    // or the parent task uses prepartitioned output, full connectivity means we partition the output into as many pieces as there are output tasks.
                    int partitionCount = outputStage.TaskCount * channelConfig.PartitionsPerTask;
                    for( int x = 1; x <= partitionCount; ++x )
                    {
                        _outputPartitionIds.Add(TaskId.CreateTaskIdString(channelConfig.OutputStage, x));
                    }
                }
                else
                {
                    // This task is a child task in a compound, which means partitioning has already been done. It is assumed the task counts are identical (should've been checked at job creation time)
                    // and this task produces only one file that is meant for the output task with a matching number. If there are multiple input stages for that output task, it is assumed they 
                    // all produce the same partitions.
                    _outputPartitionIds.Add(TaskId.CreateTaskIdString(channelConfig.OutputStage, taskExecution.Context.TaskId.PartitionNumber));
                }
            }

            CompressionType = taskExecution.Context.GetSetting(CompressionTypeSetting, taskExecution.JetClient.Configuration.FileChannel.CompressionType);
        }

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public abstract RecordWriter<T> CreateRecordWriter<T>();

        /// <summary>
        /// Gets the task execution utility for the task that this channel is for.
        /// </summary>
        protected TaskExecutionUtility TaskExecution { get; private set; }

        /// <summary>
        /// Gets the IDs of the partitions that this channel writes output to.
        /// </summary>
        /// <value>The output ids.</value>
        protected ReadOnlyCollection<string> OutputPartitionIds
        {
            get
            {
                if( _outputIdsReadOnlyWrapper == null )
                    System.Threading.Interlocked.CompareExchange(ref _outputIdsReadOnlyWrapper, _outputPartitionIds.AsReadOnly(), null);
                return _outputIdsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Gets the compression type to use for the channel.
        /// </summary>
        protected CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Creates the partitioner for the output channel.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>An object implementing <see cref="IPartitioner{T}"/> that will partition the channel's output.</returns>
        protected IPartitioner<T> CreatePartitioner<T>()
        {
            IPartitioner<T> partitioner;
            if( TaskExecution.Context.StageConfiguration.InternalPartitionCount > 1 && TaskExecution.Context.StageConfiguration.IsOutputPrepartitioned )
                partitioner = new PrepartitionedPartitioner<T>();
            else
                partitioner = (IPartitioner<T>)JetActivator.CreateInstance(TaskExecution.Context.StageConfiguration.OutputChannel.PartitionerType.ReferencedType, TaskExecution);
            return partitioner;
        }
    }
}
