// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides configuration for the <see cref="MergeRecordReader{T}"/>.
    /// </summary>
    public class MergeRecordReaderConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the maxinum number of file inputs to use in a single merge pass.
        /// </summary>
        /// <value>The maximum number of file inputs in a single merge pass. The default value is 100.</value>
        [ConfigurationProperty("maxFileInputs", DefaultValue = 100, IsRequired = false, IsKey = false)]
        [IntegerValidator(MinValue=2, MaxValue=Int32.MaxValue)]
        public int MaxFileInputs
        {
            get { return (int)this["maxFileInputs"]; }
            set { this["maxFileInputs"] = value; }
        }

        /// <summary>
        /// Gets or sets the usage level of the channel's memory storage that will trigger a merge pass.
        /// </summary>
        /// <value>The memory storage trigger level, between 0 and 1. The default value is 0.6.</value>
        [ConfigurationProperty("memoryStorageTriggerLevel", DefaultValue = 0.6f, IsRequired = false, IsKey = false)]
        public float MemoryStorageTriggerLevel
        {
            get { return (float)this["memoryStorageTriggerLevel"]; }
            set { this["memoryStorageTriggerLevel"] = value; }
        }

        /// <summary>
        /// Gets or sets the buffer size to use for each input file.
        /// </summary>
        /// <value>The size of the read buffer for each merge stream.</value>
        [ConfigurationProperty("mergeStreamReadBufferSize", DefaultValue = "1MB", IsRequired = false, IsKey = false)]
        public BinarySize MergeStreamReadBufferSize
        {
            get { return (BinarySize)this["mergeStreamReadBufferSize"]; }
            set { this["mergeStreamReadBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether all in-memory inputs must be merged and purged to disk before the final pass.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if all in-memory inputs must be merged and purged to disk before the final pass; otherwise, <see langword="false"/>.
        /// 	The default value is <see langword="false"/>.
        /// </value>
        [ConfigurationProperty("purgeMemoryBeforeFinalPass", DefaultValue = false, IsRequired = false, IsKey = false)]
        public bool PurgeMemoryBeforeFinalPass
        {
            get { return (bool)this["purgeMemoryBeforeFinalPass"]; }
            set { this["purgeMemoryBeforeFinalPass"] = value; }
        }
    }
}
