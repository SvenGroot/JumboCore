// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Jet;

namespace JetShell.Commands
{
    [Command("metrics"), Description("Displays generic information about the Jumbo Jet cluster.")]
    class PrintMetricsCommand : JetShellCommand
    {
        public override int Run()
        {
            var metrics = JetClient.JobServer.GetMetrics();
            if (RunningJobs)
            {
                foreach (var jobId in metrics.RunningJobs)
                    Console.WriteLine(jobId);
            }
            else
                metrics.PrintMetrics(Console.Out);

            return 0;
        }

        [CommandLineArgument, Description("Print a list of running jobs.")]
        public bool RunningJobs { get; set; }
    }
}
