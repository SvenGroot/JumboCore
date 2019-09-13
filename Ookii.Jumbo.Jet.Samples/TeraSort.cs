// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Samples.IO;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Dfs;
using System.Runtime.InteropServices;
using Ookii.CommandLine;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet.Samples
{
    /// <summary>
    /// Job runner for GraySort, which sorts <see cref="GenSortRecord"/> records in the input.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This is a sort implementation according to the rules for the GraySort benchmark, see http://www.hpl.hp.com/hosted/sortbenchmark/.
    /// </para>
    /// </remarks>
    [Description("Sorts the input file or files containing data in the gensort format.")]
    public class TeraSort : JobBuilderJob
    {
        /// <summary>
        /// Gets or sets the input path.
        /// </summary>
        /// <value>
        /// The input path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 0), Description("The input file or directory containing the data to be sorted.")]
        public string InputPath { get; set; }

        /// <summary>
        /// Gets or sets the output path.
        /// </summary>
        /// <value>
        /// The output path.
        /// </value>
        [CommandLineArgument(IsRequired = true, Position = 1), Description("The output directory where the sorted data will be written.")]
        public string OutputPath { get; set; }

        /// <summary>
        /// Gets or sets the merge tasks.
        /// </summary>
        /// <value>
        /// The merge tasks.
        /// </value>
        [CommandLineArgument(Position = 2, DefaultValue = 0), Description("The number of merge tasks to use. The default value is the cluster capacity.")]
        public int MergeTasks { get; set; }

        /// <summary>
        /// Gets or sets the sample size used to determine the partitioner's split points.
        /// </summary>
        [CommandLineArgument(DefaultValue = 10000), Description("The number of records to sample in order to determine the partitioner's split points. The default is 10000.")]
        public int SampleSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of merge inputs for a single merge pass.
        /// </summary>
        /// <value>The maximum number of file merge inputs.</value>
        [CommandLineArgument, Description("The maximum number of inputs for a single merge pass. If unspecified, Jumbo Jet's default value will be used.")]
        public int MaxMergeInputs { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions per merge task.
        /// </summary>
        /// <value>The number of partitions per task.</value>
        [CommandLineArgument(DefaultValue = 1), Description("The number of partitions per merge task. The default is 1.")]
        public int PartitionsPerTask { get; set; }

        /// <summary>
        /// Constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected override void BuildJob(JobBuilder job)
        {
            var input = job.Read(InputPath, typeof(GenSortRecordReader));

            var sorted = job.SpillSort(input);
            sorted.InputChannel.PartitionerType = typeof(RangePartitioner);
            sorted.InputChannel.TaskCount = MergeTasks;
            sorted.InputChannel.PartitionsPerTask = PartitionsPerTask;

            WriteOutput(sorted, OutputPath, typeof(GenSortRecordWriter));
        }

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        protected override void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
            // Sample the input and create the partition split points for the RangePartitioner.
            string partitionFileName = FileSystemClient.Path.Combine(job.Path, RangePartitioner.SplitFileName);
            var input = (from stage in jobConfiguration.Stages
                            where stage.DataInput != null
                            select stage.DataInput).SingleOrDefault();
            RangePartitioner.CreatePartitionFile(FileSystemClient, partitionFileName, input, MergeTasks, SampleSize);
        }
    }
}
