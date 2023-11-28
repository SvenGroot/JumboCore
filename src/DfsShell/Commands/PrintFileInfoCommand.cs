// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("fileinfo"), Description("Prints information about the specified file.")]
partial class PrintFileInfoCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The path of the file on the DFS.")]
    public string Path { get; set; }

    public override int Run()
    {
        var file = Client.GetFileInfo(Path);
        if (file == null)
        {
            Console.WriteLine("File not found.");
            return 1;
        }

        file.PrintFileInfo(Console.Out);
        return 0;
    }
}
