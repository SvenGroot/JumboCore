// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using Ookii.CommandLine;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Samples.IO;

namespace Ookii.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for ValSort, which validates the sort order of its input.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The ValSort job checks if its entire input is correctly sorted, and calculates the infinite-precision sum of the
    ///   CRC32 checksum of each record and the number of duplicate records.
    /// </para>
    /// <para>
    ///   The output of this job is a file containing a diagnostic message indicating whether the output was sorted,
    ///   identical to the one given by the original C version of valsort (see http://www.ordinal.com/gensort.html). For
    ///   convenience, the job runner will print this message to the console.
    /// </para>
    /// </remarks>
    [Description("Validates whether the input, using GenSort records, is correctly sorted.")]
    public class ValSort : JobBuilderJob
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(ValSort));

        private string _outputFile;

        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 0), Description("The input file or directory containing the data to validate.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 1), Description("The output directory where the results of the validation will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether verbose logging of unsorted record locations is enabled in the combiner task.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if verbose logging is enabled in the combiner task; otherwise, <see langword="false"/>. The default value is <see langword="false"/>.
        /// </value>
        [CommandLineArgument, JobSetting, Description("Enables verbose logging of where unsorted records occured in the combiner task.")]
        public bool VerboseLogging { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(GenSortRecordReader));
            var validatedSegments = job.Process<GenSortRecord, ValSortRecord>(input, ValidateRecords);
            var sorted = job.SpillSort(validatedSegments);
            sorted.InputChannel.PartitionCount = 1;
            var validated = job.Process<ValSortRecord, string>(sorted, ValidateResults);
            validated.InputChannel.ChannelType = ChannelType.Pipeline;
            WriteOutput(validated, OutputPath, typeof(TextRecordWriter<>));
        }

        /// <summary>
        /// Overrides <see cref="JobBuilderJob.OnJobCreated"/>.
        /// </summary>
        /// <param name="job"></param>
        /// <param name="jobConfiguration"></param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            _outputFile = FileDataOutput.GetOutputPath(jobConfiguration.GetStage("ValidateResultsTaskStage"), 1);
        }

        /// <summary>
        /// Called after the job finishes.
        /// </summary>
        /// <param name="success"><see langword="true"/> if the job completed successfully; <see langword="false"/> if the job failed.</param>
        public override void FinishJob(bool success)
        {
            if (success)
            {
                Console.WriteLine();
                try
                {
                    using (Stream stream = FileSystemClient.OpenFile(_outputFile))
                    using (StreamReader reader = new StreamReader(stream))
                    {
                        Console.WriteLine(reader.ReadToEnd());
                    }
                }
                catch (System.IO.FileNotFoundException)
                {
                    Console.WriteLine("The output file was not found (did the job fail?).");
                }
            }
            base.FinishJob(success);
        }

        /// <summary>
        /// Validates the records.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        [AllowRecordReuse]
        public static void ValidateRecords(RecordReader<GenSortRecord> input, RecordWriter<ValSortRecord> output, TaskContext context)
        {
            Crc32Checksum crc = new Crc32Checksum();
            long recordCrc;
            UInt128 checksum = UInt128.Zero;
            UInt128 duplicates = UInt128.Zero;
            UInt128 unsorted = UInt128.Zero;
            UInt128 count = UInt128.Zero;
            GenSortRecord first = null;
            GenSortRecord prev = null;
            UInt128? firstUnordered = null;
            foreach (GenSortRecord record in input.EnumerateRecords())
            {
                crc.Reset();
                crc.Update(record.RecordBuffer);
                recordCrc = crc.Value;
                checksum += new UInt128(0, (ulong)recordCrc);
                if (prev == null)
                {
                    first = record;
                }
                else
                {
                    int diff = prev.CompareTo(record);
                    if (diff == 0)
                        ++duplicates;
                    else if (diff > 0)
                    {
                        if (firstUnordered == null)
                            firstUnordered = count;
                        ++unsorted;
                    }
                }
                prev = record;
                ++count;
            }

            FileTaskInput taskInput = (FileTaskInput)context.TaskInput;
            _log.InfoFormat("Input file {0} split offset {1} size {2} contains {3} unordered records.", taskInput.Path, taskInput.Offset, taskInput.Size, unsorted);

            ValSortRecord result = new ValSortRecord()
            {
                InputId = taskInput.Path,
                InputOffset = taskInput.Offset,
                FirstKey = first.ExtractKeyBytes(),
                LastKey = prev.ExtractKeyBytes(),
                Records = count,
                UnsortedRecords = unsorted,
                FirstUnsorted = firstUnordered != null ? firstUnordered.Value : UInt128.Zero,
                Checksum = checksum,
                Duplicates = duplicates
            };
            output.WriteRecord(result);
        }

        /// <summary>
        /// Validates the results.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="output">The output.</param>
        /// <param name="context">The context.</param>
        /// <remarks>
        ///   Does not allow record reuse; the function stores the previous instance.
        /// </remarks>
        public static void ValidateResults(RecordReader<ValSortRecord> input, RecordWriter<string> output, TaskContext context)
        {
            ValSortRecord prev = null;
            UInt128 checksum = UInt128.Zero;
            UInt128 unsortedRecords = UInt128.Zero;
            UInt128 duplicates = UInt128.Zero;
            UInt128 records = UInt128.Zero;
            UInt128? firstUnsorted = null;

            foreach (ValSortRecord record in input.EnumerateRecords())
            {
                bool verbose = context.GetSetting("ValSort.VerboseLogging", false);

                if (prev != null)
                {
                    int diff = GenSortRecord.CompareKeys(prev.LastKey, record.FirstKey);
                    if (diff == 0)
                        ++duplicates;
                    else if (diff > 0)
                    {
                        if (verbose)
                            _log.InfoFormat("Input parts {0}-{1} and {2}-{3} are not sorted in relation to each other.", prev.InputId, prev.InputOffset, record.InputId, record.InputOffset);

                        if (firstUnsorted == null)
                            firstUnsorted = records;
                        ++unsortedRecords;
                    }
                }

                if (verbose && record.UnsortedRecords.High64 > 0 || record.UnsortedRecords.Low64 > 0)
                    _log.InfoFormat("Input part {0}-{1} has {2} unsorted records.", prev.InputId, prev.InputOffset, record.UnsortedRecords);

                unsortedRecords += record.UnsortedRecords;
                checksum += record.Checksum;
                duplicates += record.Duplicates;
                if (firstUnsorted == null && record.UnsortedRecords != UInt128.Zero)
                {
                    firstUnsorted = records + record.FirstUnsorted;
                }
                records += record.Records;

                prev = record;
            }

            if (unsortedRecords != UInt128.Zero)
            {
                output.WriteRecord(string.Format("First unordered record is record {0}", firstUnsorted.Value));
            }
            output.WriteRecord(string.Format("Records: {0}", records));
            output.WriteRecord(string.Format("Checksum: {0}", checksum.ToHexString()));
            if (unsortedRecords == UInt128.Zero)
            {
                output.WriteRecord(string.Format("Duplicate keys: {0}", duplicates));
                output.WriteRecord("SUCCESS - all records are in order");
            }
            else
            {
                output.WriteRecord(string.Format("ERROR - there are {0} unordered records", unsortedRecords));
            }
        }
    }
}
