// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("metrics"), Description("Prints general information about the DFS.")]
    class PrintMetricsCommand : DfsShellCommand
    {
        public override void Run()
        {
            DfsClient client = Client as DfsClient;
            if( client == null )
                Console.WriteLine("No metrics for the configured file system.");
            else
            {
                DfsMetrics metrics = client.NameServer.GetMetrics();
                metrics.PrintMetrics(Console.Out);
            }
        }
    }
}
