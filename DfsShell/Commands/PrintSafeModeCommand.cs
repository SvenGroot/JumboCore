// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("safemode"), Description("Checks whether safemode is on or off.")]
    class PrintSafeModeCommand : DfsShellCommand
    {
        public override void Run()
        {
            DfsClient client = Client as DfsClient;
            if( client != null && client.NameServer.SafeMode )
                Console.WriteLine("Safe mode is ON.");
            else
                Console.WriteLine("Safe mode is OFF.");
        }
    }
}
