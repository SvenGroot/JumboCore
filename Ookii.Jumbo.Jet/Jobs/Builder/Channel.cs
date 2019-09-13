// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents the channel between two operations.
    /// </summary>
    public sealed class Channel : IOperationOutput
    {
        private readonly IJobBuilderOperation _sender;
        private readonly IJobBuilderOperation _receiver;
        private Type _partitionerType;
        private Type _multiInputRecordReaderType;
        private int _taskCount;
        private int _partitionsPerTask = 1;
        private SettingsDictionary _settings;

        /// <summary>
        /// Initializes a new instance of the <see cref="Channel"/> class.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="receiver">The receiver.</param>
        public Channel(IJobBuilderOperation sender, IJobBuilderOperation receiver)
        {
            if( sender == null )
                throw new ArgumentNullException("sender");
            if( receiver == null )
                throw new ArgumentNullException("receiver");
            _sender = sender;
            _receiver = receiver;
            _sender.SetOutput(this);
        }

        /// <summary>
        /// Gets the operation that is writing records to this channel.
        /// </summary>
        /// <value>
        /// The sending operation.
        /// </value>
        public IJobBuilderOperation Sender
        {
            get { return _sender; }
        }

        /// <summary>
        /// Gets the operation that is reading records from this channel.
        /// </summary>
        /// <value>
        /// The receiving operation.
        /// </value>
        public IJobBuilderOperation Receiver
        {
            get { return _receiver; }
        }

        /// <summary>
        /// Gets the type of the records that can be written to this channel.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        public Type RecordType
        {
            get { return _sender.RecordType; }
        }

        /// <summary>
        /// Gets or sets the channel type.
        /// </summary>
        /// <value>
        /// The channel type, or <see langword="null"/> to let the <see cref="JobBuilder"/> decide. The default value is <see langword="null"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If the channel type is unspecified, the channel will default to a file channel unless there is only 1 partition and 1 input task or the
        ///   number of partitions is unspecified and the input uses <see cref="Tasks.EmptyTask{T}"/>, in which case it will use a pipeline channel.
        /// </para>
        /// </remarks>
        public ChannelType? ChannelType { get; set; }
        
        /// <summary>
        /// Gets or sets the partitioner to use to spread the records across the output tasks.
        /// </summary>
        /// <value>
        /// The <see cref="Type"/> of the partitioner to use, or <see langword="null"/> to use to the default hash partitioner. The default value is <see langword="null"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this value is set to a type that is a generic type definition, the type is constructed using the channel's record type.
        /// </para>
        /// </remarks>
        public Type PartitionerType
        {
            get { return _partitionerType; }
            set
            {
                if( value != null )
                {
                    if( value.IsGenericTypeDefinition )
                        value = value.MakeGenericType(RecordType);

                    Type partitionerInterfaceType = value.FindGenericInterfaceType(typeof(IPartitioner<>), true);
                    if( RecordType != partitionerInterfaceType.GetGenericArguments()[0] )
                        throw new ArgumentException("The partitioner's record type doesn't match the channel's record type.");
                }
                _partitionerType = value;
            }
        }

        /// <summary>
        /// Gets or sets the type of the channel's multi-input record reader.
        /// </summary>
        /// <value>
        /// The type of the multi-input record reader, or <see langword="null"/> to use the default for this channel type.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this value is set to a type that is a generic type definition, the type is constructed using the channel's record type.
        /// </para>
        /// </remarks>
        public Type MultiInputRecordReaderType
        {
            get { return _multiInputRecordReaderType; }
            set 
            {
                if( value != null )
                {
                    if( value.IsGenericTypeDefinition )
                        value = value.MakeGenericType(RecordType);

                    Type baseType = value.FindGenericBaseType(typeof(MultiInputRecordReader<>), true);
                    if( RecordType != baseType.GetGenericArguments()[0] )
                        throw new ArgumentException("The multi-input record reader's record type doesn't match the channel's record type.");
                }
                _multiInputRecordReaderType = value; 
            }
        }
        

        /// <summary>
        /// Gets or sets the number of partitions to create
        /// </summary>
        /// <value>
        /// The number of partitions to create, or zero to let the <see cref="JobBuilder"/> decide. The default value is 0.
        /// </value>
        public int PartitionCount
        {
            get { return _taskCount * _partitionsPerTask; }
            set
            {
                if( value < 0 )
                    throw new ArgumentOutOfRangeException("value", "The partition count must be 0 or higher.");
                if( value > 0 && value % _partitionsPerTask != 0 )
                    throw new InvalidOperationException("The total number of partitions must be divisible by the number of partitions per task.");
                _taskCount = value / _partitionsPerTask;
            }
        }

        /// <summary>
        /// Gets or sets the number of partitions per task.
        /// </summary>
        /// <value>The number of partitions per task, or 0 to let the <see cref="JobBuilder"/> decide. The default value is 0.</value>
        public int PartitionsPerTask
        {
            get { return _partitionsPerTask; }
            set
            {
                if( value < 1 )
                    throw new ArgumentOutOfRangeException("value", "The number of partitions per task must be 1 or higher.");

                _partitionsPerTask = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of tasks created for this channel's receiving stage.
        /// </summary>
        /// <value>
        /// The number of tasks, or 0 to let the <see cref="JobBuilder"/> decide. The default value is 0.
        /// </value>
        public int TaskCount
        {
            get { return _taskCount; }
            set 
            {
                if( value < 0 )
                    throw new ArgumentOutOfRangeException("value", "The task count must be 0 or higher.");
                _taskCount = value;
            }
        }

        /// <summary>
        /// Gets or sets the partition assignment method used when <see cref="PartitionsPerTask"/> is larger than 1.
        /// </summary>
        /// <value>
        /// One of the values of the <see cref="Ookii.Jumbo.Jet.Channels.PartitionAssignmentMethod"/> enumeration.
        /// </value>
        public PartitionAssignmentMethod PartitionAssignmentMethod { get; set; }

        /// <summary>
        /// Gets the settings for the channel's sending stage.
        /// </summary>
        /// <value>
        /// A <see cref="SettingsDictionary"/> containing the settings.
        /// </value>
        /// <remarks>
        /// <para>
        ///   Channel settings are applied to the stage that writes to this channel (the sending stage). In the case of a two-step operation, this can
        ///   either be the operation's original input stage (if no additional step is created), or the additionally created first step.
        /// </para>
        /// <para>
        ///   If no actual channel is created (empty task replacement was used), these settings are not applied at all.
        /// </para>
        /// </remarks>
        public SettingsDictionary Settings
        {
            get { return _settings ?? (_settings = new SettingsDictionary()); }
        }

        /// <summary>
        /// Creates an <see cref="InputStageInfo"/> for this channel.
        /// </summary>
        /// <param name="overrideSender">An alternative sender for this channel. May be <see langword="null"/>.</param>
        /// <returns>
        /// An <see cref="InputStageInfo"/>.
        /// </returns>
        public InputStageInfo CreateInput(StageConfiguration overrideSender = null)
        {
            StageConfiguration sender = overrideSender;
            if( sender == null )
            {
                if( Sender.Stage == null )
                    throw new InvalidOperationException("The sending stage for this channel has not been compiled.");
                sender = Sender.Stage;
            }

            return new InputStageInfo(sender)
                {
                    ChannelType = ChannelType ?? GetDefaultChannelType(sender),
                    PartitionsPerTask = PartitionsPerTask,
                    PartitionerType = PartitionerType,
                    PartitionAssignmentMethod = PartitionAssignmentMethod,
                    MultiInputRecordReaderType = MultiInputRecordReaderType
                };
        }
        
        void IOperationOutput.ApplyOutput(FileSystemClient client, StageConfiguration stage)
        {
            // Nothing.
        }

        private ChannelType GetDefaultChannelType(StageConfiguration sender)
        {
            return ((PartitionCount <= 1 && sender.Root.TaskCount == 1) || (PartitionCount == 0 && JobBuilderCompiler.IsEmptyTask(sender.TaskType.ReferencedType))) 
                ? Channels.ChannelType.Pipeline : Channels.ChannelType.File;
        }
    }
}
