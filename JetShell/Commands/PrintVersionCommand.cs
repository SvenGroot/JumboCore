// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.Jumbo;

namespace JetShell.Commands
{
    [ShellCommand("version"), Description("Shows version information.")]
    class PrintVersionCommand : JetShellCommand
    {
        [CommandLineArgument, Description("Display only the revision number rather than the full version.")]
        public bool Revision { get; set; }

        public override void Run()
        {
            if (Revision)
                Console.WriteLine(RuntimeEnvironment.JumboVersion.Revision);
            else
                Console.WriteLine("Jumbo {0} ({1})", RuntimeEnvironment.JumboVersion, RuntimeEnvironment.JumboConfiguration);
        }
    }
}
