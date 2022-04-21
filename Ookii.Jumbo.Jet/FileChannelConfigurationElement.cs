// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration information for the file channel.
    /// </summary>
    public class FileChannelConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the buffer size to use for input to push and pull tasks.
        /// </summary>
        [ConfigurationProperty("readBufferSize", DefaultValue = "64KB", IsRequired = false, IsKey = false)]
        public BinarySize ReadBufferSize
        {
            get { return (BinarySize)this["readBufferSize"]; }
            set { this["readBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the buffer size used by the file output channel to write to intermediate files.
        /// </summary>
        [ConfigurationProperty("writeBufferSize", DefaultValue = "64KB", IsRequired = false, IsKey = false)]
        public BinarySize WriteBufferSize
        {
            get { return (BinarySize)this["writeBufferSize"]; }
            set { this["writeBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether intermediate files should be deleted after the task or job is finished.
        /// </summary>
        [ConfigurationProperty("deleteIntermediateFiles", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool DeleteIntermediateFiles
        {
            get { return (bool)this["deleteIntermediateFiles"]; }
            set { this["deleteIntermediateFiles"] = value; }
        }

        /// <summary>
        /// Gets or sets the maximum size of the the in-memory input storage.
        /// </summary>
        [ConfigurationProperty("memoryStorageSize", DefaultValue = "100MB", IsRequired = false, IsKey = false)]
        public BinarySize MemoryStorageSize
        {
            get { return (BinarySize)this["memoryStorageSize"]; }
            set { this["memoryStorageSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the amount of time to wait for memory to become available before shuffling to disk.
        /// </summary>
        [ConfigurationProperty("memoryStorageWaitTimeout", DefaultValue = 60000, IsRequired = false, IsKey = false)]
        public int MemoryStorageWaitTimeout
        {
            get { return (int)this["memoryStorageWaitTimeout"]; }
            set { this["memoryStorageWaitTimeout"] = value; }
        }

        /// <summary>
        /// Gets or sets the type of compression to use for intermediate files.
        /// </summary>
        [ConfigurationProperty("compressionType", DefaultValue = CompressionType.None, IsRequired = false, IsKey = false)]
        public CompressionType CompressionType
        {
            get { return (CompressionType)this["compressionType"]; }
            set { this["compressionType"] = value; }
        }

        /// <summary>
        /// Gets or sets the size of the spill buffer used for file output channels.
        /// </summary>
        /// <value>The size of the single file output buffer.</value>
        [ConfigurationProperty("spillBufferSize", DefaultValue = "100MB", IsRequired = false, IsKey = false)]
        public BinarySize SpillBufferSize
        {
            get { return (BinarySize)this["spillBufferSize"]; }
            set { this["spillBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the percentage of spill buffer usage at which the <see cref="SpillRecordWriter{T}"/> for a file output channel using 
        /// <see cref="FileChannelOutputType.Spill"/> and <see cref="FileChannelOutputType.SortSpill"/> should start writing the buffer to disk.
        /// </summary>
        /// <value>The single file output buffer limit.</value>
        [ConfigurationProperty("spillBufferLimit", DefaultValue = 0.8f, IsRequired = false, IsKey = false)]
        public float SpillBufferLimit
        {
            get { return (float)this["spillBufferLimit"]; }
            set { this["spillBufferLimit"] = value; }
        }

        /// <summary>
        /// Gets or sets the minimum number of spills needed for the <see cref="SortSpillRecordWriter{T}"/> for the a file output channel using
        /// <see cref="FileChannelOutputType.SortSpill"/> to run the combiner (if there is one) during the merge phase.
        /// </summary>
        /// <value>
        /// The minimum number of spills needed for the combiner to run during the merge phase. The default value is 3.
        /// </value>
        [ConfigurationProperty("spillSortMinSpillsForCombineDuringMerge", DefaultValue = 3, IsRequired = false, IsKey = false)]
        public int SpillSortMinSpillsForCombineDuringMerge
        {
            get { return (int)this["spillSortMinSpillsForCombineDuringMerge"]; }
            set { this["spillSortMinSpillsForCombineDuringMerge"] = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether checksums are calculated and verified for the intermediate data.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if checksums are enabled; otherwise, <see langword="false"/>. The default value is <see langword="true"/>.
        /// </value>
        [ConfigurationProperty("enableChecksum", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool EnableChecksum
        {
            get { return (bool)this["enableChecksum"]; }
            set { this["enableChecksum"] = value; }
        }
    }
}
