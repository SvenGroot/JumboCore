// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [Command("rm"), Description("Deletes a file or directory from the DFS.")]
    class DeleteCommand : DfsShellCommand
    {
        private readonly string _path;

        public DeleteCommand([Description("The path of the file or directory on the DFS to delete."), ArgumentName("Path")] string path)
        {
            ArgumentNullException.ThrowIfNull(path);

            _path = path;
        }

        [CommandLineArgument, Description("Recursively delete all children of a directory.")]
        public bool Recursive { get; set; }

        public override int Run()
        {
            if (!Client.Delete(_path, Recursive))
            {
                Console.Error.WriteLine("Path did not exist.");
                return 1;
            }

            return 0;
        }
    }
}
