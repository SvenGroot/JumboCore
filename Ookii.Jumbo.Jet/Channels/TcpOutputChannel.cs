// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents the writing end of a TCP channel between two tasks.
    /// </summary>
    public sealed class TcpOutputChannel : OutputChannel, IHasMetrics
    {
        /// <summary>
        /// The key in the stage or job settings that can be used to specify the size of the spill buffer. The setting should have the type <see cref="BinarySize"/>,
        /// and the default value is the value of <see cref="TcpChannelConfigurationElement.SpillBufferSize"/>.
        /// </summary>
        public const string SpillBufferSizeSettingKey = "TcpOutputChannel.SpillBufferSize";

        /// <summary>
        /// The key in the stage or job settings that can be used to specify the size of the spill buffer. The setting should have the type <see cref="Single"/>,
        /// and the default value is the value of <see cref="TcpChannelConfigurationElement.SpillBufferLimit"/>.
        /// </summary>
        public const string SpillBufferLimitSettingKey = "TcpOutputChannel.SpillBufferLimit";

        /// <summary>
        /// The key in the stage or job settings that can be used to specify whether the connections to the receiving stage tasks are kept open.
        /// The setting should have the type <see cref="Boolean"/> and the default value is the value of <see cref="TcpChannelConfigurationElement.ReuseConnections"/>.
        /// </summary>
        public const string ReuseConnectionsSettingKey = "TcpOutputChannel.ReuseConnections";

        private IRecordWriter _writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpOutputChannel"/> class.
        /// </summary>
        /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
        public TcpOutputChannel(TaskExecutionUtility taskExecution)
            : base(taskExecution)
        {
        }

        /// <summary>
        /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
        /// </summary>
        /// <typeparam name="T">The type of the records.</typeparam>
        /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public override RecordWriter<T> CreateRecordWriter<T>()
        {
            if (_writer != null)
                throw new InvalidOperationException("Channel record writer was already created.");

            bool reuseConnections = TaskExecution.Context.GetSetting(ReuseConnectionsSettingKey, TaskExecution.JetClient.Configuration.TcpChannel.ReuseConnections);
            BinarySize spillBufferSize = TaskExecution.Context.GetSetting(SpillBufferSizeSettingKey, TaskExecution.JetClient.Configuration.TcpChannel.SpillBufferSize);
            float spillBufferLimit = TaskExecution.Context.GetSetting(SpillBufferLimitSettingKey, TaskExecution.JetClient.Configuration.TcpChannel.SpillBufferLimit);
            if (spillBufferSize.Value < 0 || spillBufferSize.Value > Int32.MaxValue)
                throw new ConfigurationErrorsException("Invalid output buffer size: " + spillBufferSize.Value);
            if (spillBufferLimit < 0.1f || spillBufferLimit > 1.0f)
                throw new ConfigurationErrorsException("Invalid output buffer limit: " + spillBufferLimit);

            IPartitioner<T> partitioner = CreatePartitioner<T>();
            partitioner.Partitions = OutputPartitionIds.Count;
            TcpChannelRecordWriter<T> writer = new TcpChannelRecordWriter<T>(TaskExecution, reuseConnections, partitioner, (int)spillBufferSize.Value, (int)(spillBufferSize.Value * spillBufferLimit));
            _writer = writer;

            return writer;
        }

        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <value>The local bytes read.</value>
        public long LocalBytesRead
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The local bytes written.</value>
        public long LocalBytesWritten
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the number of bytes read over the network.
        /// </summary>
        /// <value>The network bytes read.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesRead
        {
            get { return 0L; }
        }

        /// <summary>
        /// Gets the number of bytes written over the network.
        /// </summary>
        /// <value>The network bytes written.</value>
        /// <remarks>Only channels should normally use this property.</remarks>
        public long NetworkBytesWritten
        {
            get { return _writer == null ? 0L : _writer.BytesWritten; }
        }
    }
}
