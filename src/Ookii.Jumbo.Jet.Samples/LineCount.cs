// Copyright (c) Sven Groot (Ookii.org)
using System.ComponentModel;
using System.Linq;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace Ookii.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for line count.
    /// </summary>
    [GeneratedParser]
    [Description("Counts the number of lines in the input file or files.")]
    public partial class LineCount : JobBuilderJob
    {
        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The input file or directory containing the text to perform the line count on.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(Position = 0, IsRequired = true), Description("The output directory where the results will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(LineRecordReader));
            var counted = job.Process<Utf8String, long>(input, CountLines);
            var summed = job.Process<long, long>(input, SumLineCount);
            summed.InputChannel.PartitionCount = 1;
            WriteOutput(summed, OutputPath, typeof(TextRecordWriter<>));
        }

        /// <summary>
        /// Counts the number of lines.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse]
        public static void CountLines(RecordReader<Utf8String> input, RecordWriter<long> output)
        {
            output.WriteRecord(input.EnumerateRecords().Count());
        }

        /// <summary>
        /// Sums the line count.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        [AllowRecordReuse]
        public static void SumLineCount(RecordReader<long> input, RecordWriter<long> output)
        {
            output.WriteRecord(input.EnumerateRecords().Sum());
        }
    }
}
