// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo.Jet;

namespace JetShell.Commands
{
    [ShellCommand("metrics"), Description("Displays generic information about the Jumbo Jet cluster.")]
    class PrintMetricsCommand : JetShellCommand
    {
        public override void Run()
        {
            var metrics = JetClient.JobServer.GetMetrics();
            if (RunningJobs)
            {
                foreach (var jobId in metrics.RunningJobs)
                    Console.WriteLine(jobId);
            }
            else
                metrics.PrintMetrics(Console.Out);
        }

        [CommandLineArgument, Description("Print a list of running jobs.")]
        public bool RunningJobs { get; set; }
    }
}
