// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("waitsafemode"), Description("Waits until the name server leaves safe mode.")]
    class WaitSafeModeCommand : DfsShellCommand
    {
        private readonly int _timeout;

        public WaitSafeModeCommand([Description("The timeout of the wait operation in milliseconds. The default is to wait indefinitely."), ArgumentName("Timeout")] int timeout = Timeout.Infinite)
        {
            _timeout = timeout;
        }

        public override void Run()
        {
            DfsClient dfsClient = Client as DfsClient;
            if (dfsClient == null || dfsClient.WaitForSafeModeOff(_timeout))
                Console.WriteLine("Safe mode is OFF.");
            else
                Console.WriteLine("Safe mode is ON.");
        }
    }
}
