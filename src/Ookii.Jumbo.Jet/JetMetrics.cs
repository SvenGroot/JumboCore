// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Represents information about the current state of the Jet distributed execution engine.
/// </summary>
[GeneratedValueWriter]
public partial class JetMetrics
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetMetrics"/> class.
    /// </summary>
    public JetMetrics()
    {
    }

    /// <summary>
    /// Gets or sets the address of the job server.
    /// </summary>
    /// <value>The address of the job server.</value>
    public ServerAddress? JobServer { get; set; }

    /// <summary>
    /// Gets or sets the IDs of the running jobs.
    /// </summary>
    public List<Guid> RunningJobs { get; } = new();

    /// <summary>
    /// Gets or sets the IDs of jobs that have successfully finished.
    /// </summary>
    public List<Guid> FinishedJobs { get; } = new();

    /// <summary>
    /// Gets or sets the IDs of jobs that have failed.
    /// </summary>
    public List<Guid> FailedJobs { get; } = new();

    /// <summary>
    /// Gets or sets a list of task servers registered with the system.
    /// </summary>
    public List<TaskServerMetrics> TaskServers { get; } = new();

    /// <summary>
    /// Gets or sets the total task capacity.
    /// </summary>
    public int Capacity { get; set; }

    /// <summary>
    /// Gets or sets the name of the scheduler being used.
    /// </summary>
    public string? Scheduler { get; set; }

    /// <summary>
    /// Prints the metrics.
    /// </summary>
    /// <param name="writer">The <see cref="TextWriter"/> to print the metrics to.</param>
    public void PrintMetrics(TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
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
        foreach (var item in list)
        {
            writer.WriteLine("  {0}", item);
        }
    }
}
