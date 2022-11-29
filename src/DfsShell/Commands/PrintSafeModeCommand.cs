// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [Command("safemode"), Description("Checks whether safemode is on or off.")]
    class PrintSafeModeCommand : DfsShellCommand
    {
        public override int Run()
        {
            var client = Client as DfsClient;
            if (client != null && client.NameServer.SafeMode)
                Console.WriteLine("Safe mode is ON.");
            else
                Console.WriteLine("Safe mode is OFF.");

            return 0;
        }
    }
}
