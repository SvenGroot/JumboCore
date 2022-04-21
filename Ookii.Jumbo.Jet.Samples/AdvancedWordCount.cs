// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace Ookii.Jumbo.Jet.Samples
{
    /// <summary>
    /// JobBuilder for the advanced version of WordCount.
    /// </summary>
    /// <remarks>
    /// This sample is a more advanced version of <see cref="WordCount"/> that demonstrates some
    /// more advanced features.
    /// </remarks>
    [Description("Alternative version of WordCount that demonstrates some more advanced features of Jumbo.")]
    public class AdvancedWordCount : JobBuilderJob
    {
        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 0), Description("The input file or directory containing the input text (must be utf-8).")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 1), Description("The directory where the output will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to use a case-insensitive comparison for the words.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if a case-insensitive comparison should be used; otherwise, <see langword="false" />.
        /// </value>
        [CommandLineArgument, Description("Perform a case-insensitive comparison on the words.")]
        public bool CaseInsensitive { get; set; }

        /// <summary>
        /// Gets or sets the path of the ignore patterns file.
        /// </summary>
        /// <value>
        /// The path of the ignore patterns file.
        /// </value>
        [CommandLineArgument, JobSetting, Description("The path of a file containing regular expression patterns that define text that should be ignored while counting.")]
        public string IgnorePatternsFile { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions for aggregation.
        /// </summary>
        /// <value>
        /// The number of partitions.
        /// </value>
        [CommandLineArgument, Description("The number of partitions to use for aggregation. Defaults to the cluster size. This does not affect the number of output files, which is always 1 for this job.")]
        public int Partitions { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder" /> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));

            var words = job.Process<Utf8String, Pair<string, int>>(input, MapWords);
            words.StageId = "WordCount";

            Type comparerType = CaseInsensitive ? typeof(OrdinalIgnoreCaseStringComparer) : null;

            var aggregated = job.GroupAggregate<string, int>(words, AggregateCounts, comparerType);
            aggregated.InputChannel.PartitionCount = Partitions;
            words.StageId = "WordCountAggregation";

            var reversed = job.Map<Pair<string, int>, Pair<int, string>>(aggregated, ReversePairs<string, int>);
            reversed.InputChannel.ChannelType = ChannelType.Pipeline;

            var sorted = job.SpillSort(reversed, typeof(InvertedRawComparer<>));
            sorted.InputChannel.TaskCount = 1;

            var output = job.Map<Pair<int, string>, Pair<string, int>>(sorted, ReversePairs<int, string>);
            output.StageId = "WordCountOutput";
            output.InputChannel.ChannelType = ChannelType.Pipeline;

            WriteOutput(output, OutputPath, typeof(TextRecordWriter<>));
        }

        /// <summary>
        /// Maps the words.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void MapWords(RecordReader<Utf8String> input, RecordWriter<Pair<string, int>> output, TaskContext context)
        {
            Regex ignorePattern = GetIgnorePattern(context);

            Pair<string, int> outputRecord = Pair.MakePair((string)null, 1);
            char[] separator = new char[] { ' ' };
            foreach (Utf8String record in input.EnumerateRecords())
            {
                string line = record.ToString();
                if (ignorePattern != null)
                    line = ignorePattern.Replace(line, " ");

                string[] words = line.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                foreach (string word in words)
                {
                    outputRecord.Key = word;
                    output.WriteRecord(outputRecord);
                }
            }
        }

        /// <summary>
        /// Aggregates the counts.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="oldValue">The old value.</param>
        /// <param name="newValue">The new value.</param>
        /// <returns></returns>
        [AllowRecordReuse]
        public static int AggregateCounts(string key, int oldValue, int newValue)
        {
            return oldValue + newValue;
        }

        /// <summary>
        /// Reverses the key and value of a pair.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="record">The record.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse]
        public static void ReversePairs<TKey, TValue>(Pair<TKey, TValue> record, RecordWriter<Pair<TValue, TKey>> output)
        {
            output.WriteRecord(Pair.MakePair(record.Value, record.Key));
        }

        private static Regex GetIgnorePattern(TaskContext context)
        {
            // It would probably be easier to just add the patterns themselves to the job settings rather than using a file, but
            // the purpose of this is to demonstrate how to use the DownloadDfsFile method.

            // TaskContext has a GetSetting method, but it searched both stage and job settings. In this case, we know
            // that the setting is in the JobConfiguration, so we just check that directly.
            string dfsPath = context.JobConfiguration.GetSetting("AdvancedWordCount.IgnorePatternsFile", null);
            if (dfsPath == null)
                return null;

            // Using DownloadDfsFile causes the TaskServer to download the file once, and all tasks on this node can then use the locally cached version.
            string path = context.DownloadDfsFile(dfsPath);
            var patterns = File.ReadLines(path).Where(line => !string.IsNullOrWhiteSpace(line)).Select(line => "(" + line.Trim() + ")");
            return new Regex(string.Join("|", patterns));
        }
    }
}
