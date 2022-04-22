// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("metrics"), Description("Prints general information about the DFS.")]
    class PrintMetricsCommand : DfsShellCommand
    {
        public override void Run()
        {
            var client = Client as DfsClient;
            if (client == null)
                Console.WriteLine("No metrics for the configured file system.");
            else
            {
                var metrics = client.NameServer.GetMetrics();
                metrics.PrintMetrics(Console.Out);
            }
        }
    }
}
