// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Contains constants for use by <see cref="MergeRecordReader{T}"/>.
    /// </summary>
    public static class MergeRecordReaderConstants
    {
        /// <summary>
        /// The name of the setting in the job or stage settings that specifies the maximum number
        /// of files to merge in one pass. This setting must be a <see cref="Int32"/> that is greater than or equal to 2.
        /// If this setting is not specified, the value of the <see cref="MergeRecordReaderConfigurationElement.MaxFileInputs"/>
        /// setting is used.
        /// </summary>
        public const string MaxFileInputsSetting = "MergeRecordReader.MaxFileInputs";

        /// <summary>
        /// The name of the setting in the <see cref="Jobs.StageConfiguration.StageSettings"/> or <see cref="Jobs.JobConfiguration.JobSettings"/>
        /// that specifies the usage level of the channel's memory storage that will trigger a merge pass. This
        /// setting must be a <see cref="Single"/> between 0 and 1. If it isn't specified, the value of the <see cref="MergeRecordReaderConfigurationElement.MemoryStorageTriggerLevel"/>
        /// property will be used.
        /// </summary>
        public const string MemoryStorageTriggerLevelSetting = "MergeRecordReader.MemoryStorageTriggerLevel";

        /// <summary>
        /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the <see cref="IComparer{T}"/>
        /// to use. If this setting is not specified, <see cref="Comparer{T}.Default"/> will be used.
        /// </summary>
        public const string ComparerSetting = "MergeRecordReader.Comparer";

        /// <summary>
        /// The name of the setting in the <see cref="Jobs.JobConfiguration.JobSettings"/> or <see cref="Jobs.StageConfiguration.StageSettings"/>
        /// that specifies whether all in-memory inputs must be merged and purged to disk before the final pass. The value
        /// of this setting must be of type <see cref="Boolean"/>. The default value is the value of <see cref="MergeRecordReaderConfigurationElement.PurgeMemoryBeforeFinalPass"/>.
        /// </summary>
        public const string PurgeMemorySettingKey = "MergeRecordReader.PurgeMemory";
    }
}
