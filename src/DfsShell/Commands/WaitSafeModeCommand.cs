// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("waitsafemode"), Description("Waits until the name server leaves safe mode.")]
partial class WaitSafeModeCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true, IncludeDefaultInUsageHelp = false)]
    [Description("The timeout of the wait operation in milliseconds. The default is to wait indefinitely.")]
    public int Timeout { get; set; } = System.Threading.Timeout.Infinite;

    public override int Run()
    {
        var dfsClient = Client as DfsClient;
        if (dfsClient == null || dfsClient.WaitForSafeModeOff(Timeout))
        {
            Console.WriteLine("Safe mode is OFF.");
            return 0;
        }
        else
        {
            Console.WriteLine("Safe mode is ON.");
            return 1;
        }
    }
}
