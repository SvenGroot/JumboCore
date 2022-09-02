// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("safemode"), Description("Checks whether safemode is on or off.")]
    class PrintSafeModeCommand : DfsShellCommand
    {
        public override void Run()
        {
            var client = Client as DfsClient;
            if (client != null && client.NameServer.SafeMode)
                Console.WriteLine("Safe mode is ON.");
            else
                Console.WriteLine("Safe mode is OFF.");
        }
    }
}
