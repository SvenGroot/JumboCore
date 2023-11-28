// Copyright (c) Sven Groot (Ookii.org)
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands;

[GeneratedParser]
[Command("mkdir"), Description("Creates a new directory on the DFS.")]
partial class CreateDirectoryCommand : DfsShellCommand
{
    [CommandLineArgument(IsPositional = true, IsRequired = true)]
    [Description("The path of the new directory to create.")]
    public string Path { get; set; }

    public override int Run()
    {
        Client.CreateDirectory(Path);
        return 0;
    }
}
