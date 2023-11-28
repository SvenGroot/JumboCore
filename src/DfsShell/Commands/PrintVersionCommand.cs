// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("version"), Description("Shows version information.")]
partial class PrintVersionCommand : DfsShellCommand
{
    [CommandLineArgument, Description("Display only the revision number rather than the full version.")]
    public bool Revision { get; set; }

    public override int Run()
    {
        if (Revision)
        {
            Console.WriteLine(RuntimeEnvironment.JumboAssemblyVersion.Revision);
        }
        else
        {
            Console.WriteLine("Jumbo {0} ({1})", RuntimeEnvironment.JumboVersion, RuntimeEnvironment.JumboConfiguration);
        }

        return 0;
    }
}
