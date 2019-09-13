// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet.Jobs.Builder;
using System.ComponentModel;
using Ookii.Jumbo.IO;
using System.IO;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Jet.Channels;
using System.Globalization;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// JobRunner for the Parallel FP-growth algorithm.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This job is an implementation of the Parallel FP Growth algorithm described in the paper
    ///   "PFP: Parallel FP-Growth for Query Recommendation" by Li et al., 2008.
    /// </para>
    /// <para>
    ///   This algorithm calculates the top-K frequent patterns for each item in the database, only
    ///   regarding patterns that have the specified minimum support.
    /// </para>
    /// <para>
    ///   The algorithm has three steps: first, it counts how often each item occurs in the input database,
    ///   filters out the infrequent features, and divides the resulting feature list into groups. Next,
    ///   it generates group-dependent transactions from the input and runs the FP-Growth algorithm on
    ///   each group. Finally, the results from each group are aggregated to form the final result.
    /// </para>
    /// <para>
    ///   The number of groups should be carefully selected so that the number of items per group it
    ///   not too large. Ideally, each group should have 5-10 items at most for a large database.
    /// </para>
    /// <para>
    ///   The input for this job should be a plain text file (or files) where each line represents
    ///   a transaction containing a space-delimited list of transactions.
    /// </para>
    /// <para>
    ///   This example demonstrates a more complicated Jumbo job, with several stages including
    ///   more than one stage with file input. It uses scheduling dependencies, group aggregation,
    ///   partition-based grouping using multiple partitions per task, dynamic partition assignment,
    ///   and custom progress providers.
    /// </para>
    /// </remarks>
    [Description("Runs the parallel FP-growth algorithm against a database of transactions.")]
    public class PFPGrowth : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(PFPGrowth));

        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 0), Description("The input file or directory containing the transaction database. The database should be a plain text file where each line is a transaction containing a space-separated list of items.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 1), Description("The output directory where the result will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets the min support.
        /// </summary>
        /// <value>The min support.</value>
        [CommandLineArgument(DefaultValue = 2), Jobs.JobSetting, Description("The minimum support of the patterns to mine.")]
        public int MinSupport { get; set; }

        /// <summary>
        /// Gets or sets the number of groups.
        /// </summary>
        /// <value>The number of groups.</value>
        [CommandLineArgument(DefaultValue = 50), Jobs.JobSetting, Description("The number of groups to create.")]
        public int Groups { get; set; }

        /// <summary>
        /// Gets or sets the number of feature count accumulator tasks.
        /// </summary>
        /// <value>The number of accumulator tasks.</value>
        [CommandLineArgument, Description("The number of feature accumulator tasks to use. Defaults to the capacity of the cluster.")]
        public int AccumulatorTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the FP growth task count.
        /// </summary>
        /// <value>The FP growth task count.</value>
        [CommandLineArgument, Description("The number of FP-growth tasks to use. The default is the capacity of the cluster.")]
        public int FPGrowthTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the pattern count.
        /// </summary>
        /// <value>The pattern count.</value>
        [CommandLineArgument(DefaultValue = 50), Jobs.JobSetting, Description("The number of patterns to return for each item.")]
        public int PatternCount { get; set; }

        /// <summary>
        /// Gets or sets the aggregate task count.
        /// </summary>
        /// <value>The aggregate task count.</value>
        [CommandLineArgument, Description("The number of aggregation tasks to use. The default is the number of nodes in the cluster.")]
        public int AggregateTaskCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating the number of partitions per task for the MineTransactions stage.
        /// </summary>
        /// <value>The partitions per task.</value>
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per task for the MineTransactions stage.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the output format is binary.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the output format is binary; otherwise, <see langword="false"/>.
        /// </value>
        [CommandLineArgument, Description("When set, the output will written using a BinaryRecordWriter rather than as text.")]
        public bool BinaryOutput { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            // Need to determine this now because we need it to validate the number of groups.
            if( FPGrowthTaskCount == 0 )
                FPGrowthTaskCount = JetClient.JobServer.GetMetrics().Capacity;

            // If the number of groups equals or is smaller than the number of partitions, we don't need to sort, because each
            // partition will get exactly one group.
            if( FPGrowthTaskCount * PartitionsPerTask < Groups )
                throw new NotSupportedException("The number of groups must be less than or equal to the number of partitions.");

            string fglistDirectory = FileSystemClient.Path.Combine(OutputPath, "fglist");
            string resultDirectory = FileSystemClient.Path.Combine(OutputPath, "output");

            var input = job.Read(InputPath, typeof(LineRecordReader));
            // Generate (feature,1) pairs for each feature in the transaction DB
            var countedFeatures = job.Process<Utf8String, Pair<Utf8String, int>>(input, CountFeatures);
            // Count the frequency of each feature.
            var aggregatedFeatureCounts = job.GroupAggregate<Utf8String, int>(countedFeatures, AccumulateFeatureCounts);
            aggregatedFeatureCounts.InputChannel.TaskCount = AccumulatorTaskCount;
            // Remove non-frequent features
            var filteredFeatureCounts = job.Process(aggregatedFeatureCounts, typeof(FeatureFilterTask));
            filteredFeatureCounts.InputChannel.ChannelType = ChannelType.Pipeline;
            // Sort and group the features.
            var groupedFeatures = job.Process(filteredFeatureCounts, typeof(FeatureGroupTask));
            groupedFeatures.InputChannel.TaskCount = 1;
            WriteOutput(groupedFeatures, fglistDirectory, typeof(BinaryRecordWriter<>));

            // Generate group-dependent transactions
            var groupedTransactions = job.Process<Utf8String, Pair<int, Transaction>>(input, GenerateGroupTransactions);
            groupedTransactions.AddSchedulingDependency(groupedFeatures);

            // Mine groups for frequent patterns.
            var patterns = job.Process(groupedTransactions, typeof(TransactionMiningTask));
            patterns.InputChannel.TaskCount = FPGrowthTaskCount;
            patterns.InputChannel.PartitionsPerTask = PartitionsPerTask;
            
            // Aggregate frequent patterns.
            var aggregatedPatterns = job.Process<Pair<int, WritableCollection<MappedFrequentPattern>>, Pair<Utf8String, WritableCollection<FrequentPattern>>>(patterns, AggregatePatterns);
            aggregatedPatterns.InputChannel.TaskCount = AggregateTaskCount;
            WriteOutput(aggregatedPatterns, resultDirectory, BinaryOutput ? typeof(BinaryRecordWriter<>) : typeof(TextRecordWriter<>));

            job.Settings.Add("PFPGrowth.FGListPath", FileSystemClient.Path.Combine(fglistDirectory, "FeatureGroupTaskStage-00001"));
        }

        /// <summary>
        /// Counts the features.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The configuration</param>
        [AllowRecordReuse]
        public static void CountFeatures(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output, TaskContext config)
        {
            var record = Pair.MakePair(new Utf8String(), 1);
            char[] separator = { ' ' };
            config.StatusMessage = "Extracting features.";
            foreach( Utf8String transaction in input.EnumerateRecords() )
            {
                string[] items = transaction.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach( string item in items )
                {
                    record.Key.Set(item);
                    output.WriteRecord(record);
                }
            }
        }

        /// <summary>
        /// Accumulates the feature counts.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="currentValue">The current value.</param>
        /// <param name="newValue">The new value.</param>
        /// <returns>The updated value.</returns>
        [AllowRecordReuse]
        public static int AccumulateFeatureCounts(Utf8String key, int currentValue, int newValue)
        {
            return currentValue + newValue;
        }

        /// <summary>
        /// Generates the group transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void GenerateGroupTransactions(RecordReader<Utf8String> input, RecordWriter<Pair<int, Transaction>> output, TaskContext config)
        {
            Dictionary<string, int> itemMapping = new Dictionary<string, int>();
            List<FGListItem> fgList = LoadFGList(config, itemMapping);

            char[] separator = { ' ' };

            foreach( Utf8String transaction in input.EnumerateRecords() )
            {
                // Extract the items for the transaction
                string[] items = transaction.ToString().Split(separator, StringSplitOptions.RemoveEmptyEntries);
                int itemCount = items.Length;
                // Map them to their item IDs.
                int mappedItemCount = 0;
                int[] mappedItems = new int[itemCount];
                for( int x = 0; x < itemCount; ++x )
                {
                    int itemId;
                    // Items that are not in the mapping are not frequent.
                    if( itemMapping.TryGetValue(items[x], out itemId) )
                    {
                        mappedItems[mappedItemCount] = itemId;
                        ++mappedItemCount;
                    }
                }

                if( mappedItemCount > 0 )
                {
                    // Sort by item ID; this ensures the items have the same order as they have in the FGList.
                    Array.Sort(mappedItems, 0, mappedItemCount);

                    int currentGroupId = -1;
                    for( int x = 0; x < mappedItemCount; ++x )
                    {
                        int groupId = fgList[mappedItems[x]].GroupId;
                        if( currentGroupId != groupId )
                        {
                            if( currentGroupId != -1 )
                            {
                                OutputGroupTransaction(output, mappedItems, currentGroupId, x, config);
                            }
                            currentGroupId = groupId;
                        }
                    }
                    OutputGroupTransaction(output, mappedItems, currentGroupId, mappedItemCount, config);
                }
            }
        }

        /// <summary>
        /// Mines the transactions.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        [AllowRecordReuse]
        public static void MineTransactions(RecordReader<Pair<int, Transaction>> input, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output, TaskContext config)
        {
            if( input.ReadRecord() )
            {
                // job settings
                int minSupport = config.JobConfiguration.GetSetting("PFPGrowth.MinSupport", 2);
                int k = config.JobConfiguration.GetSetting("PFPGrowth.PatternCount", 50);
                // stage settings
                int numGroups = config.StageConfiguration.GetSetting("PFPGrowth.Groups", 50);
                int itemCount = LoadFGList(config, null).Count;

                int maxPerGroup = itemCount / numGroups;
                if( itemCount % numGroups != 0 )
                    maxPerGroup++;
                FrequentPatternMaxHeap[] itemHeaps = null;
                while( true )
                {
                    FPTree tree;
                    int groupId;
                    if( input.HasFinished )
                        break;
                    groupId = input.CurrentRecord.Key;
                    _log.InfoFormat("Building tree for group {0}.", groupId);
                    tree = new FPTree(EnumerateGroup(input), minSupport, Math.Min((groupId + 1) * maxPerGroup, itemCount), config);

                    // The tree needs to do mining only for the items in its group.
                    itemHeaps = tree.Mine(k, false, groupId * maxPerGroup, itemHeaps);
                }

                if( itemHeaps != null )
                {
                    for( int item = 0; item < itemHeaps.Length; ++item )
                    {
                        FrequentPatternMaxHeap heap = itemHeaps[item];
                        if( heap != null )
                            heap.OutputItems(item, output);
                    }
                }
            }
        }

        /// <summary>
        /// Aggregates the patterns.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="config">The config.</param>
        /// <remarks>
        /// Does not allow record reuse (technically it could because WritableCollection doesn't reuse item instances
        /// but because that might change in the future we don't set the option here).
        /// </remarks>
        public static void AggregatePatterns(RecordReader<Pair<int, WritableCollection<MappedFrequentPattern>>> input, RecordWriter<Pair<Utf8String, WritableCollection<FrequentPattern>>> output, TaskContext config)
        {
            int k = config.JobConfiguration.GetSetting("PFPGrowth.PatternCount", 50);
            int minSupport = config.JobConfiguration.GetSetting("PFPGrowth.MinSupport", 2);

            List<FGListItem> fgList = LoadFGList(config, null);
            FrequentPatternMaxHeap[] heaps = new FrequentPatternMaxHeap[fgList.Count]; // TODO: Create a smaller list based on the number of partitions.

            foreach( Pair<int, WritableCollection<MappedFrequentPattern>> record in input.EnumerateRecords() )
            {
                config.StatusMessage = "Aggregating for item id: " + record.Key.ToString(CultureInfo.InvariantCulture);
                FrequentPatternMaxHeap heap = heaps[record.Key];
                if( heap == null )
                {
                    heap = new FrequentPatternMaxHeap(k, minSupport, true, record.Value);
                    heaps[record.Key] = heap;
                }
                else
                {
                    foreach( MappedFrequentPattern pattern in record.Value )
                    {
                        heap.Add(pattern);
                    }
                }
            }

            int patternCount = 0;
            var outputRecord = Pair.MakePair((Utf8String)null, new WritableCollection<FrequentPattern>(k));
            for( int x = 0; x < heaps.Length; ++x )
            {
                FrequentPatternMaxHeap heap = heaps[x];
                if( heap != null )
                {
                    outputRecord.Key = fgList[x].Feature;
                    outputRecord.Value.Clear();
                    PriorityQueue<MappedFrequentPattern> queue = heap.Queue;
                    while( queue.Count > 0 )
                    {
                        MappedFrequentPattern mappedPattern = queue.Dequeue();
                        outputRecord.Value.Add(new FrequentPattern(mappedPattern.Items.Select(i => fgList[i].Feature), mappedPattern.Support));
                        ++patternCount;
                    }
                    output.WriteRecord(outputRecord);
                }
            }
            _log.InfoFormat("Found {0} frequent patterns in total.", patternCount);
        }

        private static IEnumerable<ITransaction> EnumerateGroup(RecordReader<Pair<int, Transaction>> reader)
        {
            int groupId = reader.CurrentRecord.Key;
            do
            {
                //_log.Debug(reader.CurrentRecord);
                yield return reader.CurrentRecord.Value;
            } while( reader.ReadRecord() && reader.CurrentRecord.Key == groupId );
        }

        private static void OutputGroupTransaction(RecordWriter<Pair<int, Transaction>> transactionOutput, int[] mappedItems, int currentGroupId, int x, TaskContext config)
        {
            //config.StatusMessage = "Generating group dependent transactions for group: " + currentGroupId.ToString(CultureInfo.InvariantCulture);
            int[] groupItems = new int[x];
            Array.Copy(mappedItems, groupItems, x);
            transactionOutput.WriteRecord(Pair.MakePair(currentGroupId, new Transaction() { Items = groupItems, Length = groupItems.Length }));
        }

        internal static List<FGListItem> LoadFGList(TaskContext context, Dictionary<string, int> itemMapping)
        {
            string fglistPath = context.DownloadDfsFile(context.JobConfiguration.GetSetting("PFPGrowth.FGListPath", null));

            using( FileStream stream = File.OpenRead(fglistPath) )
            {
                return LoadFGList(itemMapping, stream);
            }
        }

        private static List<FGListItem> LoadFGList(Dictionary<string, int> itemMapping, Stream stream)
        {
            List<FGListItem> fgList = new List<FGListItem>();
            using( BinaryRecordReader<FGListItem> reader = new BinaryRecordReader<FGListItem>(stream, false) )
            {
                int x = 0;
                foreach( FGListItem item in reader.EnumerateRecords() )
                {
                    fgList.Add(item);
                    if( itemMapping != null )
                        itemMapping.Add(item.Feature.ToString(), x);
                    ++x;
                }
            }

            return fgList;
        }
    }
}
