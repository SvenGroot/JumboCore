// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides client access to the Jumbo Jet distributed execution engine.
    /// </summary>
    public class JetClient
    {
        private const string _jobServerObjectName = "JobServer";
        private const string _taskServerObjectName = "TaskServer";
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(JetClient));

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static JetClient()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JetClient"/> class.
        /// </summary>
        public JetClient()
            : this(JetConfiguration.GetConfiguration())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JetClient"/> class with the specified configuration.
        /// </summary>
        /// <param name="config">The configuration to use.</param>
        public JetClient(JetConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            JobServer = CreateJobServerClient(config);
            Configuration = config;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JetClient"/> class with the specified host name and port.
        /// </summary>
        /// <param name="hostName">The host name of the job server.</param>
        /// <param name="port">The port on which the job server is listening.</param>
        public JetClient(string hostName, int port)
        {
            if (hostName == null)
                throw new ArgumentNullException(nameof(hostName));
            Configuration = new JetConfiguration();
            Configuration.JobServer.HostName = hostName;
            Configuration.JobServer.Port = port;
            JobServer = CreateJobServerClient(hostName, port);
        }

        /// <summary>
        /// Gets the <see cref="IJobServerClientProtocol"/> instance used by this instance to communicate with the job server.
        /// </summary>
        public IJobServerClientProtocol JobServer { get; private set; }

        /// <summary>
        /// Gets the <see cref="JetConfiguration"/> that was used to create this instance.
        /// </summary>
        public JetConfiguration Configuration { get; private set; }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server via the heartbeat protocol.
        /// </summary>
        /// <returns>An object implementing <see cref="IJobServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerHeartbeatProtocol CreateJobServerHeartbeatClient()
        {
            return CreateJobServerHeartbeatClient(JetConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server via the heartbeat protocol
        /// using the specified configuration.
        /// </summary>
        /// <param name="config">A <see cref="JetConfiguration"/> that provides the job server configuration to use.</param>
        /// <returns>An object implementing <see cref="IJobServerHeartbeatProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerHeartbeatProtocol CreateJobServerHeartbeatClient(JetConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return CreateJobServerClientInternal<IJobServerHeartbeatProtocol>(config.JobServer.HostName, config.JobServer.Port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient()
        {
            return CreateJobServerClient(JetConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <param name="config">A <see cref="JetConfiguration"/> that provides the job server configuration to use.</param>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient(JetConfiguration config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return CreateJobServerClientInternal<IJobServerClientProtocol>(config.JobServer.HostName, config.JobServer.Port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with a job server.
        /// </summary>
        /// <param name="hostName">The host name of the job server.</param>
        /// <param name="port">The port at which the job server is listening.</param>
        /// <returns>An object implementing <see cref="IJobServerClientProtocol"/> that is a proxy class for
        /// communicating with the job server via RPC.</returns>
        public static IJobServerClientProtocol CreateJobServerClient(string hostName, int port)
        {
            if (hostName == null)
                throw new ArgumentNullException(nameof(hostName));

            return CreateJobServerClientInternal<IJobServerClientProtocol>(hostName, port);
        }

        /// <summary>
        /// Creates a client object that can be used by a task host to communicate with its task server.
        /// </summary>
        /// <param name="port">The port at which the task server is listening.</param>
        /// <returns>An object implementing <see cref="ITaskServerUmbilicalProtocol"/> that is a proxy class for
        /// communicating with the task server via RPC.</returns>
        public static ITaskServerUmbilicalProtocol CreateTaskServerUmbilicalClient(int port)
        {
            return CreateTaskServerClientInternal<ITaskServerUmbilicalProtocol>("localhost", port);
        }

        /// <summary>
        /// Creates a client object that can be used to communicate with its task server server.
        /// </summary>
        /// <param name="address">The address of the task server.</param>
        /// <returns>An object implementing <see cref="ITaskServerClientProtocol"/> that is a proxy class for
        /// communicating with the task server via RPC.</returns>
        public static ITaskServerClientProtocol CreateTaskServerClient(ServerAddress address)
        {
            if (address == null)
                throw new ArgumentNullException(nameof(address));

            return CreateTaskServerClientInternal<ITaskServerClientProtocol>(address.HostName, address.Port);
        }

        /// <summary>
        /// Creates a new job, stores the job configuration and the specified files on the DFS, and runs the job.
        /// </summary>
        /// <param name="config">The <see cref="JobConfiguration"/> for the job.</param>
        /// <param name="files">The local paths of the files to store in the job directory on the DFS. This should include the assembly containing the task classes.</param>
        /// <returns>An instance of the <see cref="Job"/> class describing the job that was started.</returns>
        /// <remarks>
        /// This function uses the application's configuration to create a <see cref="FileSystemClient"/> to access the DFS.
        /// </remarks>
        public Job RunJob(JobConfiguration config, params string[] files)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            var job = JobServer.CreateJob();
            _log.InfoFormat("Created job {{{0}}}", job.JobId);
            RunJob(job, config, files);
            return job;
        }

        /// <summary>
        /// Waits until the specified job is complete, printing progress updates to the console.
        /// </summary>
        /// <param name="jobId">The job ID.</param>
        /// <param name="pollIntervalMilliseconds">The poll interval in milliseconds.</param>
        /// <returns><see langword="true"/> if the job completed successfully; otherwise, <see langword="false"/>.</returns>
        public bool WaitForJobCompletion(Guid jobId, int pollIntervalMilliseconds = 1000)
        {
            JobStatus status = null;
            string previousStatus = null;
            do
            {
                Thread.Sleep(pollIntervalMilliseconds);
                status = JobServer.GetJobStatus(jobId);
                var statusString = status.ToString(CultureInfo.CurrentCulture);
                if (statusString != previousStatus)
                {
                    Console.WriteLine(statusString);
                    previousStatus = statusString;
                }
            } while (!status.IsFinished);

            Console.WriteLine();
            if (status.IsSuccessful)
                Console.WriteLine("Job completed.");
            else
                Console.WriteLine("Job failed.");
            Console.WriteLine("Start time: {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.StartTime.ToLocalTime());
            Console.WriteLine("End time:   {0:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff}", status.EndTime.ToLocalTime());
            var duration = status.EndTime - status.StartTime;
            Console.WriteLine("Duration:   {0} ({1}s)", duration, duration.TotalSeconds);

            return status.IsSuccessful;
        }

        /// <summary>
        /// Stores the job configuration and the specified files on the DFS, and runs the job.
        /// </summary>
        /// <param name="job">The job to run.</param>
        /// <param name="config">The <see cref="JobConfiguration"/> for the job.</param>
        /// <param name="files">The local paths of the files to store in the job directory on the DFS. This should include the assembly containing the task classes.</param>
        /// <returns>An instance of the <see cref="Job"/> class describing the job that was started.</returns>
        /// <remarks>
        /// This function uses the application's configuration to create a <see cref="FileSystemClient"/> to access the DFS.
        /// </remarks>
        public void RunJob(Job job, JobConfiguration config, params string[] files)
        {
            RunJob(job, config, FileSystemClient.Create(), files);
        }

        /// <summary>
        /// Creates a new job, stores the job configuration and the specified files on the DFS using the specified <see cref="FileSystemClient"/>, and runs the job.
        /// </summary>
        /// <param name="config">The <see cref="JobConfiguration"/> for the job.</param>
        /// <param name="fileSystemClient">A <see cref="FileSystemClient"/> used to access the Jumbo DFS.</param>
        /// <param name="files">The local paths of the files to store in the job directory on the DFS. This should include the assembly containing the task classes.</param>
        /// <returns>An instance of the <see cref="Job"/> class describing the job that was started.</returns>
        public Job RunJob(JobConfiguration config, FileSystemClient fileSystemClient, params string[] files)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (fileSystemClient == null)
                throw new ArgumentNullException(nameof(fileSystemClient));

            var job = JobServer.CreateJob();
            _log.InfoFormat("Created job {{{0}}}", job.JobId);
            RunJob(job, config, fileSystemClient, files);
            return job;
        }

        /// <summary>
        /// Stores the job configuration and the specified files on the DFS using the specified <see cref="FileSystemClient"/>, and runs the job.
        /// </summary>
        /// <param name="job">The job to run.</param>
        /// <param name="config">The <see cref="JobConfiguration"/> for the job.</param>
        /// <param name="fileSystemClient">A <see cref="FileSystemClient"/> used to access the Jumbo DFS.</param>
        /// <param name="files">The local paths of the files to store in the job directory on the DFS. This should include the assembly containing the task classes.</param>
        /// <returns>An instance of the <see cref="Job"/> class describing the job that was started.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void RunJob(Job job, JobConfiguration config, FileSystemClient fileSystemClient, params string[] files)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (fileSystemClient == null)
                throw new ArgumentNullException(nameof(fileSystemClient));

            try
            {
                config.Validate();

                var configFilePath = job.GetJobConfigurationFilePath(fileSystemClient);
                _log.InfoFormat("Saving job configuration to DFS file {0}.", configFilePath);
                using (var stream = fileSystemClient.CreateFile(configFilePath))
                {
                    config.SaveXml(stream);
                }

                // Save split files for all stages with input.
                foreach (var stage in config.Stages)
                {
                    if (stage.DataInput != null)
                    {
                        TaskInputUtility.WriteTaskInputs(fileSystemClient, job.Path, stage.StageId, stage.DataInput.TaskInputs);
                    }
                }

                // Upload additional files
                if (files != null)
                {
                    foreach (var file in files)
                    {
                        _log.InfoFormat("Uploading local file {0} to DFS directory {1}.", file, job.Path);
                        fileSystemClient.UploadFile(file, job.Path);
                    }
                }

                _log.InfoFormat("Running job {0}.", job.JobId);
                try
                {
                    File.WriteAllText("jumbo_last_job.txt", job.JobId.ToString());
                }
                catch
                {
                    // Don't care if this fails.
                }
                JobServer.RunJob(job.JobId);
            }
            catch
            {
                try
                {
                    // Remove job from the pending queue if submission failed
                    JobServer.AbortJob(job.JobId);
                }
                catch
                {
                    // Don't care about abortion failure, and we don't want that to mask the real exception
                }

                throw;
            }
        }

        /// <summary>
        /// Waits until the specified job has finished.
        /// </summary>
        /// <param name="jobId">The job ID of the job to wait for.</param>
        /// <param name="millisecondsTimeout">The maximum amount of time to wait.</param>
        /// <param name="millisecondsInterval">The interval at which to check for job completion.</param>
        /// <returns><see langword="true"/> if the job finished, or <see langword="null"/> if the timeout expired.</returns>
        public bool WaitForJobCompletion(Guid jobId, int millisecondsTimeout, int millisecondsInterval)
        {
            var sw = new Stopwatch();
            sw.Start();
            var status = JobServer.GetJobStatus(jobId);
            if (status == null)
                throw new ArgumentException("Unknown job ID.", nameof(jobId));

            while (!status.IsFinished && (millisecondsTimeout == Timeout.Infinite || sw.ElapsedMilliseconds < millisecondsTimeout))
            {
                Thread.Sleep(millisecondsInterval);
                status = JobServer.GetJobStatus(jobId);
            }

            sw.Stop();
            return status.IsFinished;
        }

        internal static IJobServerTaskProtocol CreateJobServerTaskClient(JetConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            return CreateJobServerClientInternal<IJobServerTaskProtocol>(configuration.JobServer.HostName, configuration.JobServer.Port);
        }

        private static T CreateJobServerClientInternal<T>(string hostName, int port)
        {
            return RpcHelper.CreateClient<T>(hostName, port, _jobServerObjectName);
        }

        private static T CreateTaskServerClientInternal<T>(string hostName, int port)
        {
            return RpcHelper.CreateClient<T>(hostName, port, _taskServerObjectName);
        }
    }
}
