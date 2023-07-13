// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [Command("mv"), Description("Moves a file or directory on the DFS.")]
    class MoveCommand : DfsShellCommand
    {
        [CommandLineArgument(IsPositional = true, IsRequired = true)]
        [Description("The path of the file or directory on the DFS to move.")]
        public string Path { get; set; }

        [CommandLineArgument(IsPositional = true, IsRequired = true)]
        [Description("The path on the DFS to move the file or directory to.")]
        public string Destination { get; set; }

        public override int Run()
        {
            Client.Move(Path, Destination);
            return 0;
        }
    }
}
