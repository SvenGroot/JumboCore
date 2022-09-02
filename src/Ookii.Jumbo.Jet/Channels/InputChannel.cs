// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Provides base functionality for <see cref="IInputChannel"/> implementations.
    /// </summary>
    public abstract class InputChannel : IInputChannel
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(InputChannel));

        private readonly List<string> _inputTaskIds = new List<string>();
        private ReadOnlyCollection<string> _inputTaskIdsReadOnlyWrapper;
        private readonly List<int> _partitions = new List<int>();
        private readonly ReadOnlyCollection<int> _partitionsReadOnlyWrapper;

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
        public event EventHandler<MemoryStorageFullEventArgs> MemoryStorageFull;

        /// <summary>
        /// Initializes a new instance of the <see cref="InputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        /// <param name="inputStage">The input stage that this file channel reads from.</param>
        protected InputChannel(TaskExecutionUtility taskExecution, StageConfiguration inputStage)
        {
            ArgumentNullException.ThrowIfNull(taskExecution);
            ArgumentNullException.ThrowIfNull(inputStage);

            _partitionsReadOnlyWrapper = _partitions.AsReadOnly();
            TaskExecution = taskExecution;
            InputStage = inputStage;
            // Match the compression type of the input stage.
            if (inputStage.TryGetSetting(FileOutputChannel.CompressionTypeSetting, out             // Match the compression type of the input stage.
            CompressionType type))
                CompressionType = type;
            else
                CompressionType = taskExecution.Context.JobConfiguration.GetSetting(FileOutputChannel.CompressionTypeSetting, taskExecution.JetClient.Configuration.FileChannel.CompressionType);
            // The type of the records in the intermediate files will be the output type of the input stage, which usually matches the input type of the output stage but
            // in the case of a join it may not.
            InputRecordType = inputStage.TaskType.ReferencedType.FindGenericInterfaceType(typeof(ITask<,>)).GetGenericArguments()[1];

            GetInputTaskIdsFull();
        }

        /// <summary>
        /// Gets the configuration of the input channel.
        /// </summary>
        /// <value>The configuration of the input channel.</value>
        public ChannelConfiguration Configuration
        {
            get { return InputStage.OutputChannel; }
        }

        /// <summary>
        /// Gets the input stage of this channel.
        /// </summary>
        /// <value>The <see cref="StageConfiguration"/> of the input stage.</value>
        public StageConfiguration InputStage { get; private set; }

        /// <summary>
        /// Gets the task execution utility for the task that this channel provides input for.
        /// </summary>
        protected TaskExecutionUtility TaskExecution { get; private set; }

        /// <summary>
        /// Gets the last set of partitions assigned to this channel.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This property returns the set of partitions passed in the last
        ///   call to <see cref="AssignAdditionalPartitions"/>, or the initial
        ///   partitions if that method hasn't been called.
        /// </para>
        /// </remarks>
        public ReadOnlyCollection<int> ActivePartitions
        {
            get
            {
                return _partitionsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the input channel uses memory storage to store inputs.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the channel uses memory storage; otherwise, <see langword="false"/>.
        /// </value>
        public abstract bool UsesMemoryStorage { get; }

        /// <summary>
        /// Gets the current memory storage usage level.
        /// </summary>
        /// <value>The memory storage usage level, between 0 and 1.</value>
        /// <remarks>
        /// 	<para>
        /// The <see cref="MemoryStorageLevel"/> will always be 0 if <see cref="UsesMemoryStorage"/> is <see langword="false"/>.
        /// </para>
        /// 	<para>
        /// If an input was too large to be stored in memory, <see cref="MemoryStorageLevel"/> will be 1 regardless of
        /// the actual level.
        /// </para>
        /// </remarks>
        public abstract float MemoryStorageLevel { get; }

        /// <summary>
        /// Gets the compression type used by the channel.
        /// </summary>
        protected CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Gets the type of the records create by the input task of this channel.
        /// </summary>
        protected Type InputRecordType { get; private set; }

        /// <summary>
        /// Gets a collection of input task IDs.
        /// </summary>
        protected ReadOnlyCollection<string> InputTaskIds
        {
            get
            {
                if (_inputTaskIdsReadOnlyWrapper == null)
                    System.Threading.Interlocked.CompareExchange(ref _inputTaskIdsReadOnlyWrapper, _inputTaskIds.AsReadOnly(), null);
                return _inputTaskIdsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Creates a <see cref="RecordReader{T}"/> from which the channel can read its input.
        /// </summary>
        /// <returns>A <see cref="RecordReader{T}"/> for the channel.</returns>
        public abstract IRecordReader CreateRecordReader();

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
        public virtual void AssignAdditionalPartitions(IList<int> additionalPartitions)
        {
            ArgumentNullException.ThrowIfNull(additionalPartitions);
            if (additionalPartitions.Count == 0)
                throw new ArgumentException("The list of partitions is empty.", nameof(additionalPartitions));

            _partitions.Clear();
            _partitions.AddRange(additionalPartitions);
        }

        /// <summary>
        /// Creates a record reader of the type indicated by the channel.
        /// </summary>
        /// <returns>An instance of a class implementin
        /// g <see cref="IMultiInputRecordReader"/>.</returns>
        protected IMultiInputRecordReader CreateChannelRecordReader()
        {
            var multiInputRecordReaderType = InputStage.OutputChannel.MultiInputRecordReaderType.ReferencedType;
            _log.InfoFormat(System.Globalization.CultureInfo.CurrentCulture, "Creating MultiRecordReader of type {3} for {0} inputs, allow record reuse = {1}, buffer size = {2}.", InputTaskIds.Count, TaskExecution.Context.StageConfiguration.AllowRecordReuse, TaskExecution.JetClient.Configuration.FileChannel.ReadBufferSize, multiInputRecordReaderType);
            var bufferSize = (multiInputRecordReaderType.IsGenericType && multiInputRecordReaderType.GetGenericTypeDefinition() == typeof(MergeRecordReader<>)) ? (int)TaskExecution.JetClient.Configuration.MergeRecordReader.MergeStreamReadBufferSize : (int)TaskExecution.JetClient.Configuration.FileChannel.ReadBufferSize;
            // We're not using JetActivator to create the object because we need to delay calling NotifyConfigurationChanged until after InputStage was set.
            var partitions = TaskExecution.GetPartitions();
            _partitions.AddRange(partitions);
            var reader = (IMultiInputRecordReader)Activator.CreateInstance(multiInputRecordReaderType, partitions, _inputTaskIds.Count, TaskExecution.Context.StageConfiguration.AllowRecordReuse, bufferSize, CompressionType);
            var channelReader = reader as IChannelMultiInputRecordReader;
            if (channelReader != null)
                channelReader.Channel = this;
            JetActivator.ApplyConfiguration(reader, TaskExecution.FileSystemClient.Configuration, TaskExecution.JetClient.Configuration, TaskExecution.Context);
            return reader;
        }

        /// <summary>
        /// Raises the <see cref="E:MemoryStorageFull" /> event.
        /// </summary>
        /// <param name="e">The <see cref="EventArgs"/> instance containing the event data.</param>
        protected virtual void OnMemoryStorageFull(MemoryStorageFullEventArgs e)
        {
            var handler = MemoryStorageFull;
            if (handler != null)
                handler(this, e);
        }

        private void GetInputTaskIdsFull()
        {
            // We add only the root task IDs, we ignore child tasks.
            var stage = InputStage.Root;
            for (var x = 1; x <= stage.TaskCount; ++x)
            {
                var taskId = new TaskId(stage.StageId, x);
                _inputTaskIds.Add(taskId.ToString());
            }
        }
    }
}
