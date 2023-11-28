// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("metrics"), Description("Prints general information about the DFS.")]
partial class PrintMetricsCommand : DfsShellCommand
{
    public override int Run()
    {
        var client = Client as DfsClient;
        if (client == null)
        {
            Console.WriteLine("No metrics for the configured file system.");
            return 1;
        }
        else
        {
            var metrics = client.NameServer.GetMetrics();
            metrics.PrintMetrics(Console.Out);
        }

        return 0;
    }
}
