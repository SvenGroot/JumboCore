// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Represents information about the current state of the Jet distributed execution engine.
    /// </summary>
    [Serializable]
    public class JetMetrics
    {
        private readonly ExtendedCollection<Guid> _runningJobs = new ExtendedCollection<Guid>();
        private readonly ExtendedCollection<Guid> _finishedJobs = new ExtendedCollection<Guid>();
        private readonly ExtendedCollection<Guid> _failedJobs = new ExtendedCollection<Guid>();
        private readonly ExtendedCollection<TaskServerMetrics> _taskServers = new ExtendedCollection<TaskServerMetrics>();

        /// <summary>
        /// Gets or sets the addrses of the job server.
        /// </summary>
        /// <value>The address of the job server.</value>
        public ServerAddress JobServer { get; set; }

        /// <summary>
        /// Gets or sets the IDs of the running jobs.
        /// </summary>
        public Collection<Guid> RunningJobs
        {
            get { return _runningJobs; }
        }

        /// <summary>
        /// Gets or sets the IDs of jobs that have successfully finished.
        /// </summary>
        public Collection<Guid> FinishedJobs
        {
            get { return _finishedJobs; }
        }

        /// <summary>
        /// Gets or sets the IDs of jobs that have failed.
        /// </summary>
        public Collection<Guid> FailedJobs
        {
            get { return _failedJobs; }
        }

        /// <summary>
        /// Gets or sets a list of task servers registered with the system.
        /// </summary>
        public Collection<TaskServerMetrics> TaskServers
        {
            get { return _taskServers; }
        }

        /// <summary>
        /// Gets or sets the total task capacity.
        /// </summary>
        public int Capacity { get; set; }

        /// <summary>
        /// Gets or sets the name of the scheduler being used.
        /// </summary>
        public string Scheduler { get; set; }

        /// <summary>
        /// Prints the metrics.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> to print the metrics to.</param>
        public void PrintMetrics(TextWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            writer.WriteLine("Job server: {0}", JobServer);
            writer.WriteLine("Running jobs: {0}", RunningJobs.Count);
            PrintList(writer, RunningJobs);
            writer.WriteLine("Finished jobs: {0}", FinishedJobs.Count);
            PrintList(writer, FinishedJobs);
            writer.WriteLine("Failed jobs: {0}", FailedJobs.Count);
            PrintList(writer, FailedJobs);
            writer.WriteLine("Capacity: {0}", Capacity);
            writer.WriteLine("Scheduler: {0}", Scheduler);
            writer.WriteLine("Task servers: {0}", TaskServers.Count);
            PrintList(writer, TaskServers);
        }

        private static void PrintList<T>(TextWriter writer, IEnumerable<T> list)
        {
            foreach( var item in list )
                writer.WriteLine("  {0}", item);
        }
    }
}
