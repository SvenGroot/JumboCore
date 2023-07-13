// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [GeneratedParser]
    [Command("rm"), Description("Deletes a file or directory from the DFS.")]
    partial class DeleteCommand : DfsShellCommand
    {
        [CommandLineArgument(IsPositional = true, IsRequired = true)]
        [Description("The path of the file or directory on the DFS to delete.")]
        public string Path { get; set; }


        [CommandLineArgument, Description("Recursively delete all children of a directory.")]
        public bool Recursive { get; set; }

        public override int Run()
        {
            if (!Client.Delete(Path, Recursive))
            {
                Console.Error.WriteLine("Path did not exist.");
                return 1;
            }

            return 0;
        }
    }
}
