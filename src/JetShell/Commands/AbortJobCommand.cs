// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace JetShell.Commands;

[GeneratedParser]
[Command("abort"), Description("Aborts a running job.")]
partial class AbortJobCommand : JetShellCommand
{
    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The job ID of the job to abort.")]
    public Guid JobId { get; set; }

    public override int Run()
    {
        if (JetClient.JobServer.AbortJob(JobId))
        {
            Console.WriteLine("Aborted job {0:B}.", JobId);
            return 0;
        }
        else
        {
            Console.WriteLine("Job {0:B} was not found or not running.", JobId);
            return 1;
        }
    }
}
