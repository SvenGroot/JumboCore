// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Base class for job runners that use the <see cref="JobBuilder"/> to create the job configuration.
    /// </summary>
    public abstract class JobBuilderJob : BaseJobRunner
    {
        /// <summary>
        /// Gets or sets a value indicating whether the job runner will only create and print the job configuration, instead of running the job.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the job runner will only create the configuration; otherwise, <see langword="false"/>.
        /// </value>
        [CommandLineArgument, Description("Don't run the job, but only create the configuration and write it to the specified file. Use this to test if your job builder job is creating the correct configuration without running the job. Note there can still be side-effects such as output directories on the file system being created. If the OverwriteOutput switch is specified, the output directory will still be erased!")]
        [ValueDescription("FileName")]
        public string ConfigOnly { get; set; }

        /// <summary>
        /// Starts the job.
        /// </summary>
        /// <returns>The job ID of the newly created job.</returns>
        public sealed override Guid RunJob()
        {
            PromptIfInteractive(true);

            var fileSystemClient = FileSystemClient.Create(DfsConfiguration);
            var jetClient = new JetClient(JetConfiguration);

            var builder = new JobBuilder(fileSystemClient, jetClient);
            try
            {
                BuildJob(builder);

                var config = builder.CreateJob();

                if (config.JobName == null)
                    config.JobName = GetType().Name; // Use the class name as the job's friendly name, if it hasn't been set explicitly.

                ApplyJobPropertiesAndSettings(config);

                if (ConfigOnly != null)
                {
                    using (Stream stream = File.Create(ConfigOnly))
                    {
                        config.SaveXml(stream);
                    }
                    return Guid.Empty;
                }
                else
                {
                    var job = jetClient.JobServer.CreateJob();

                    OnJobCreated(job, config);
                    jetClient.RunJob(job, config, fileSystemClient, builder.AssemblyLocations.ToArray());

                    return job.JobId;
                }
            }
            finally
            {
                builder.TaskBuilder.DeleteAssembly(); // This is safe to do after the assembly has been uploaded to the DFS.
            }
        }

        /// <summary>
        /// When implemented in a derived class, constructs the job configuration using the specified job builder.
        /// </summary>
        /// <param name="job">The <see cref="JobBuilder"/> used to create the job.</param>
        protected abstract void BuildJob(JobBuilder job);

        /// <summary>
        /// Called when the job has been created on the job server, but before running it.
        /// </summary>
        /// <param name="job">The <see cref="Job"/> instance describing the job.</param>
        /// <param name="jobConfiguration">The <see cref="JobConfiguration"/> that will be used when the job is started.</param>
        /// <remarks>
        ///   Override this method if you want to make changes to the job configuration (e.g. add settings) or upload additional files to the DFS.
        /// </remarks>
        protected virtual void OnJobCreated(Job job, JobConfiguration jobConfiguration)
        {
        }

        /// <summary>
        /// Writes the result of the operation to the DFS using this instance's settings for <see cref="BaseJobRunner.BlockSize"/> and <see cref="BaseJobRunner.ReplicationFactor"/>.
        /// </summary>
        /// <param name="operation">The operation whose output to write.</param>
        /// <param name="outputPath">The output path.</param>
        /// <param name="recordWriterType">The type of the record writer to use.</param>
        /// <returns>
        /// A <see cref="FileOutput"/>.
        /// </returns>
        protected FileOutput WriteOutput(IJobBuilderOperation operation, string outputPath, Type recordWriterType)
        {
            ArgumentNullException.ThrowIfNull(operation);
            var output = operation.JobBuilder.Write(operation, outputPath, recordWriterType);
            output.BlockSize = (int)BlockSize;
            output.ReplicationFactor = ReplicationFactor;
            CheckAndCreateOutputPath(outputPath);
            return output;
        }
    }
}
