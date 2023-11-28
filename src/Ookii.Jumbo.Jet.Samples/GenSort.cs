// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Samples.IO;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Samples;

/// <summary>
/// Job runner for GenSort, which generates input records for various sort benchmarks.
/// </summary>
/// <remarks>
/// <para>
///   The GenSort job produces a deterministic range of input records in the <see cref="Ookii.Jumbo.Jet.Samples.IO.GenSortRecord"/> format.
/// </para>
/// <para>
///   The output of the GenSort job is byte-for-byte identical to that of the ASCII records created by the
///   2009 version of the official gensort data generator provided for the graysort sort benchmark. The original
///   C version can be found at http://www.ordinal.com/gensort.html.
/// </para>
/// </remarks>
[GeneratedParser]
[Description("Generates input records for the TeraSort job.")]
public partial class GenSort : JobBuilderJob
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(GenSort));

    /// <summary>
    /// Gets or sets the output path.
    /// </summary>
    /// <value>
    /// The output path.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The output directory where the generated data will be written.")]
    public required string OutputPath { get; set; }

    /// <summary>
    /// Gets or sets the record count.
    /// </summary>
    /// <value>
    /// The record count.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The total number of records to generate."), Jobs.JobSetting]
    public required ulong RecordCount { get; set; }

    /// <summary>
    /// Gets or sets the task count.
    /// </summary>
    /// <value>
    /// The task count.
    /// </value>
    [CommandLineArgument(IsPositional = true), Description("The number of tasks to use to generate the data.")]
    public required int TaskCount { get; set; }

    /// <summary>
    /// Gets or sets the start record.
    /// </summary>
    /// <value>
    /// The start record.
    /// </value>
    [CommandLineArgument, Description("The record number to start at."), Jobs.JobSetting]
    public ulong StartRecord { get; set; }

    /// <summary>
    /// Constructs the job configuration using the specified job builder.
    /// </summary>
    /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
    protected override void BuildJob(JobBuilder job)
    {
        if (RecordCount < 1)
        {
            throw new ArgumentOutOfRangeException("RecordCount", "You must generate at least one record.");
        }

        if (TaskCount < 1)
        {
            throw new ArgumentOutOfRangeException("TaskCount", "You must use at least one generator task.");
        }

        ulong countPerTask = RecordCount / (ulong)TaskCount;
        ulong remainder = RecordCount % (ulong)TaskCount;
        _log.InfoFormat("Generating {0} records with {1} tasks, {2} records per task, remainder {3}.", RecordCount, TaskCount, countPerTask, remainder);

        var generated = job.Generate<GenSortRecord>(TaskCount, Generate);
        WriteOutput(generated, OutputPath, typeof(GenSortRecordWriter));
    }

    /// <summary>
    /// Generates records.
    /// </summary>
    /// <param name="output">The output.</param>
    /// <param name="context">The context.</param>
    public static void Generate(RecordWriter<GenSortRecord> output, ProgressContext context)
    {
        ulong startRecord = context.TaskContext!.GetSetting("GenSort.StartRecord", 0UL);
        ulong count = context.TaskContext.GetSetting("GenSort.RecordCount", 0UL);

        ulong countPerTask = count / (ulong)context.TaskContext.StageConfiguration.TaskCount;
        int taskNum = context.TaskContext.TaskId.TaskNumber;
        startRecord += (countPerTask * (ulong)(taskNum - 1));
        if (taskNum == context.TaskContext.StageConfiguration.TaskCount)
        {
            count = countPerTask + count % (ulong)context.TaskContext.StageConfiguration.TaskCount;
        }
        else
        {
            count = countPerTask;
        }

        _log.InfoFormat("Generating {0} records starting at number {1}.", count, startRecord);

        GenSortGenerator generator = new GenSortGenerator();
        ulong generated = 0;
        foreach (GenSortRecord record in generator.GenerateRecords(new UInt128(0, startRecord), count))
        {
            output.WriteRecord(record);
            ++generated;
            context.Progress = (float)generated / (float)count;
        }
    }
}
