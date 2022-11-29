// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [Command("ls"), Description("Displays the contents of the specified DFS directory.")]
    class ListDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public ListDirectoryCommand([Description("The path of the DFS directory. The default value is /."), ArgumentName("Path")] string path = "/")
        {
            ArgumentNullException.ThrowIfNull(path);
            _path = path;
        }

        public override int Run()
        {
            var dir = Client.GetDirectoryInfo(_path);
            if (dir == null)
            {
                Console.WriteLine("Directory not found.");
                return 1;
            }

            dir.PrintListing(Console.Out);
            return 0;
        }
    }
}
