﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Samples;

/// <summary>
/// The type of WordCount implementation to use.
/// </summary>
public enum WordCountKind
{
    /// <summary>
    /// Optimized version
    /// </summary>
    Optimized,
    /// <summary>
    /// MapReduce implementation
    /// </summary>
    MapReduce
}

/// <summary>
/// Job runner for word count.
/// </summary>
[GeneratedParser]
[Description("Counts the number of occurrences of each word in the input file or files.")]
public sealed partial class WordCount : JobBuilderJob
{
    /// <summary>
    /// Gets or sets the input path.
    /// </summary>
    /// <value>
    /// The input path.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The input file or directory containing the text to perform the word count on (must be utf-8).")]
    public required string InputPath { get; set; }

    /// <summary>
    /// Gets or sets the output path.
    /// </summary>
    /// <value>
    /// The output path.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The output directory where the results of the word count will be written.")]
    public required string OutputPath { get; set; }

    /// <summary>
    /// Gets or sets the partitions.
    /// </summary>
    /// <value>
    /// The partitions.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The number of partitions to use. Defaults to the capacity of the Jet cluster.")]
    public int Partitions { get; set; }

    /// <summary>
    /// Gets or sets the kind.
    /// </summary>
    /// <value>
    /// The kind.
    /// </value>
    [CommandLineArgument, Description("The kind of implementation to use: Optimized (default), Lambda, or MapReduce.")]
    public WordCountKind Kind { get; set; }

    /// <summary>
    /// When implemented in a derived class, constructs the job configuration using the specified job builder.
    /// </summary>
    /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
    protected override void BuildJob(JobBuilder job)
    {
        switch (Kind)
        {
        case WordCountKind.Optimized:
            BuildJobOptimized(job);
            break;

        case WordCountKind.MapReduce:
            BuildJobMapReduce(job);
            break;
        }
    }

    private void BuildJobOptimized(JobBuilder job)
    {
        var input = job.Read(InputPath, typeof(LineRecordReader));
        var pairs = job.Process<Utf8String, Pair<Utf8String, int>>(input, SplitLines);
        pairs.StageId = "WordCount";
        var counted = job.GroupAggregate(pairs, typeof(SumTask<>));
        counted.StageId = "WordCountAggregation";
        counted.InputChannel!.PartitionCount = Partitions;
        WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
    }

    private void BuildJobMapReduce(JobBuilder job)
    {
        var input = job.Read(InputPath, typeof(LineRecordReader));
        var pairs = job.Process<Utf8String, Pair<Utf8String, int>>(input, SplitLines);
        pairs.StageId = "WordCount";
        var sorted = job.SpillSortCombine<Utf8String, int>(pairs, ReduceWordCount);
        sorted.InputChannel!.PartitionCount = Partitions;
        var counted = job.Reduce<Utf8String, int, Pair<Utf8String, int>>(sorted, ReduceWordCount);
        counted.StageId = "WordCountAggregation";
        WriteOutput(counted, OutputPath, typeof(TextRecordWriter<>));
    }

    /// <summary>
    /// Splits the lines.
    /// </summary>
    /// <param name="input">The input.</param>
    /// <param name="output">The output.</param>
    [AllowRecordReuse]
    public static void SplitLines(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
    {
        // Reuse the same pair instance every time.
        Pair<Utf8String, int> record = Pair.MakePair(new Utf8String(), 1);
        Span<Range> ranges = stackalloc Range[10];
        foreach (Utf8String line in input.EnumerateRecords())
        {
            var lineSpan = line.ToString().AsSpan();
            while (true)
            {
                // Use MemoryExtensions.Split to avoid allocating a string for each word.
                var splits = lineSpan.Split(ranges, ' ', StringSplitOptions.RemoveEmptyEntries);

                // Don't write the last range in the ranges span since it could still contain
                // spaces.
                foreach (var split in ranges[..Math.Min(splits, ranges.Length - 1)])
                {
                    record.Key!.Set(lineSpan[split]);
                    output.WriteRecord(record);
                }

                if (splits != ranges.Length)
                {
                    break;
                }

                // If there are more ranges, the last item contains the remainder of the span.
                lineSpan = lineSpan[ranges[^1]];
            }
        }
    }

    /// <summary>
    /// Reduces the word count.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="values">The values.</param>
    /// <param name="output">The output.</param>
    [AllowRecordReuse(PassThrough = true)]
    public static void ReduceWordCount(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
    {
        output.WriteRecord(Pair.MakePair(key, values.Sum()));
    }
}
