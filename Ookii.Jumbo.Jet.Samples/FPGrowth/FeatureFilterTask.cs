// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Task that filters out features with too low support.
    /// </summary>
    [AllowRecordReuse(PassThrough=true)]
    public class FeatureFilterTask : PushTask<Pair<Utf8String, int>, Pair<Utf8String, int>>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FeatureFilterTask));

        private int _minSupport;
        private int _records;

        /// <summary>
        /// Processes the record.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="output">The output.</param>
        public override void ProcessRecord(Pair<Utf8String, int> record, RecordWriter<Pair<Utf8String, int>> output)
        {
            ++_records;
            if( record.Value >= _minSupport )
                output.WriteRecord(record);
        }

        /// <summary>
        /// Finishes processing. Does nothing on this task.
        /// </summary>
        /// <param name="output">The output.</param>
        public override void Finish(RecordWriter<Pair<Utf8String, int>> output)
        {
            _log.InfoFormat("{0} unique items; {1} frequent items.", _records, output.RecordsWritten);
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            _minSupport = TaskContext.JobConfiguration.GetSetting("PFPGrowth.MinSupport", 2);
        }
    }
}
