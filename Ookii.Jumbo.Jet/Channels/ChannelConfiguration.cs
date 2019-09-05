// $Id$
//
using System;
using System.Xml.Serialization;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Represents configuration information about a channel through which two tasks communicate.
    /// </summary>
    [XmlType("Channel", Namespace=JobConfiguration.XmlNamespace)]
    public class ChannelConfiguration
    {
        private int _partitionsPerTask = 1;

        /// <summary>
        /// Gets or sets the type of the channel.
        /// </summary>
        [XmlAttribute("type")]
        public ChannelType ChannelType { get; set; }

        /// <summary>
        /// Gets or sets the type of multi input record reader to use to combine the the input readers of this channel.
        /// </summary>
        public TypeReference MultiInputRecordReaderType { get; set; }
        
        /// <summary>
        /// Gets or sets the ID of the stage whose tasks that read from the channel.
        /// </summary>
        public string OutputStage { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions to create for every output task.
        /// </summary>
        [XmlAttribute("partitionsPerTask")]
        public int PartitionsPerTask
        {
            get { return _partitionsPerTask; }
            set { _partitionsPerTask = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether to disable dynamic partition assignment.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if dynamic partition assignment; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If <see cref="PartitionsPerTask"/> is larger than 1, tasks can dynamically receive additional partitions
        ///   besides those that were initially assigned to them.
        /// </para>
        /// <para>
        ///   If this property is set to <see langword="true"/>, dynamic partition assignment is disabled for
        ///   the receiving stage of this channel, and every task will only ever process the partitions it was
        ///   initially assigned.
        /// </para>
        /// </remarks>
        [XmlAttribute("disableDynamicPartitionAssignment")]
        public bool DisableDynamicPartitionAssignment { get; set; }

        /// <summary>
        /// Gets or sets the method used to assign partitions to tasks when the job is started.
        /// </summary>
        /// <value>The partition assignment method.</value>
        [XmlAttribute("partitionAssignmentMethod")]
        public PartitionAssignmentMethod PartitionAssignmentMethod { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the file channel should always use TCP downloads.
        /// </summary>
        /// <value>
        /// For a <see cref="ChannelType"/> value of <see cref="Ookii.Jumbo.Jet.Channels.ChannelType.File"/>, <see langword="true"/>
        /// to indicate that it should always use TCP to download the files even if the input task is on the same physical
        /// host as the output task; <see langword="false"/> to indicate it should access the output file directly if the
        /// input task is local. This property has no effect for other types of channels.
        /// </value>
        /// <remarks>
        /// This property is primarily used for testing of the TCP server.
        /// </remarks>
        [XmlAttribute("forceFileDownload")]
        public bool ForceFileDownload { get; set; }

        /// <summary>
        /// Gets or sets the type of partitioner to use to split the input of the channel amount its outputs.
        /// </summary>
        public TypeReference PartitionerType { get; set; }
    }
}
