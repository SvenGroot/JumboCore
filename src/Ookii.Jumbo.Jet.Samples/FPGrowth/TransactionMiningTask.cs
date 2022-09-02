// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Task for PFP growth transaction mining.
    /// </summary>
    [AllowRecordReuse, ProcessAllInputPartitions, AdditionalProgressCounter("FP growth")]
    public class TransactionMiningTask : Configurable, ITask<Pair<int, Transaction>, Pair<int, WritableCollection<MappedFrequentPattern>>>, IHasAdditionalProgress
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TransactionMiningTask));
        private int _groupsProcessed;
        private float _progress;
        private MultiPartitionRecordReader<Pair<int, Transaction>> _partitionReader;

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        public void Run(RecordReader<Pair<int, Transaction>> input, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output)
        {
            _partitionReader = input as MultiPartitionRecordReader<Pair<int, Transaction>>;
            bool reuseHeaps = TaskContext.GetSetting("PFPGrowth.ReusePatternHeaps", true);

            if (input.ReadRecord())
            {
                TaskContext config = TaskContext;
                // job settings
                int minSupport = config.JobConfiguration.GetSetting("PFPGrowth.MinSupport", 2);
                int k = config.JobConfiguration.GetSetting("PFPGrowth.PatternCount", 50);
                int numGroups = config.JobConfiguration.GetSetting("PFPGrowth.Groups", 50);

                List<FGListItem> fglist = PFPGrowth.LoadFGList(TaskContext, null);

                int maxPerGroup = fglist.Count / numGroups;
                if (fglist.Count % numGroups != 0)
                    maxPerGroup++;
                FrequentPatternMaxHeap[] itemHeaps = null;
                while (!input.HasFinished)
                {
                    int groupId;
                    groupId = input.CurrentRecord.Key;
                    string message = string.Format("Building tree for group {0}.", groupId);
                    _log.Info(message);
                    TaskContext.StatusMessage = message;
                    // Prevent fetching new partitions while building the FP tree.
                    if (_partitionReader != null)
                        _partitionReader.StopAtEndOfPartition = true;

                    using (FPTree tree = new FPTree(EnumerateGroup(input), minSupport, Math.Min((groupId + 1) * maxPerGroup, fglist.Count), TaskContext))
                    {
                        tree.ProgressChanged += new EventHandler(FPTree_ProgressChanged);

                        // The tree needs to do mining only for the items in its group.
                        itemHeaps = tree.Mine(k, false, groupId * maxPerGroup, itemHeaps);
                        _log.InfoFormat("Done mining.");
                        if (!reuseHeaps)
                        {
                            OutputPatternHeaps(output, itemHeaps);
                            itemHeaps = null;
                        }
                    }
                    ++_groupsProcessed;
                    if (_partitionReader != null)
                    {
                        // Re-enable allow additional partitions, and if we had finished before try calling ReadRecord again.
                        _partitionReader.StopAtEndOfPartition = false;
                        if (input.HasFinished)
                            input.ReadRecord();
                    }
                }

                OutputPatternHeaps(output, itemHeaps);
            }
        }

        private static void OutputPatternHeaps(RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output, FrequentPatternMaxHeap[] itemHeaps)
        {
            if (itemHeaps != null)
            {
                for (int item = 0; item < itemHeaps.Length; ++item)
                {
                    FrequentPatternMaxHeap heap = itemHeaps[item];
                    if (heap != null)
                        heap.OutputItems(item, output);
                }
                _log.InfoFormat("Done writing pattern heaps.");
            }
        }

        private static IEnumerable<ITransaction> EnumerateGroup(RecordReader<Pair<int, Transaction>> reader)
        {
            int groupId = reader.CurrentRecord.Key;
            do
            {
                //_log.Debug(reader.CurrentRecord);
                yield return reader.CurrentRecord.Value;
            } while (reader.ReadRecord() && reader.CurrentRecord.Key == groupId);
        }

        private void FPTree_ProgressChanged(object sender, EventArgs e)
        {
            MultiPartitionRecordReader<Pair<int, Transaction>> reader = _partitionReader;
            _progress = (_groupsProcessed + ((FPTree)sender).Progress) / (float)(reader == null ? 1 : reader.PartitionCount);
        }

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        public float AdditionalProgress
        {
            get { return _progress; }
        }
    }
}
