// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("ls"), Description("Displays the contents of the specified DFS directory.")]
partial class ListDirectoryCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true)]
    [Description("The path of the DFS directory.")]
    public string Path { get; set; } = "/";

    public override int Run()
    {
        var dir = Client.GetDirectoryInfo(Path);
        if (dir == null)
        {
            Console.WriteLine("Directory not found.");
            return 1;
        }

        dir.PrintListing(Console.Out);
        return 0;
    }
}
