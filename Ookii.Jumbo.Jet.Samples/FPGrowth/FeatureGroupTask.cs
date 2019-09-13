// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Sorts the feature list and divides them into groups. You should never have more than one of these in a stage.
    /// </summary>
    /// <remarks>
    /// Does not support record reuse.
    /// </remarks>
    public class FeatureGroupTask : PushTask<Pair<Utf8String, int>, FGListItem>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FeatureGroupTask));

        private readonly List<FGListItem> _fgList = new List<FGListItem>();

        /// <summary>
        /// Processes the record.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="output">The output.</param>
        public override void ProcessRecord(Pair<Utf8String, int> record, RecordWriter<FGListItem> output)
        {
            _fgList.Add(new FGListItem() { Feature = record.Key, Support = record.Value });
        }

        /// <summary>
        /// Finishes the task.
        /// </summary>
        /// <param name="output">The output.</param>
        public override void Finish(RecordWriter<FGListItem> output)
        {
            _log.InfoFormat("Sorting feature list with {0} items...", _fgList.Count);

            // Sort the list by descending support
            _fgList.Sort();

            int numGroups = TaskContext.JobConfiguration.GetSetting("PFPGrowth.Groups", 50);
            int maxPerGroup = _fgList.Count / numGroups;
            if( _fgList.Count % numGroups != 0 )
                maxPerGroup++;

            _log.InfoFormat("Dividing {0} items into {1} groups with {2} items per group...", _fgList.Count, numGroups, maxPerGroup);

            int groupSize = 0;
            int groupId = 0;
            foreach( FGListItem item in _fgList )
            {
                item.GroupId = groupId;
                if( ++groupSize == maxPerGroup )
                {
                    groupSize = 0;
                    ++groupId;
                }

                output.WriteRecord(item);
            }

            _log.Info("Done grouping.");
        }
    }
}
