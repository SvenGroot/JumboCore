// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;

namespace DfsShell.Commands
{
    [ShellCommand("ls"), Description("Displays the contents of the specified DFS directory.")]
    class ListDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public ListDirectoryCommand([Description("The path of the DFS directory. The default value is /."), ArgumentName("Path")] string path = "/")
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            _path = path;
        }

        public override void Run()
        {
            var dir = Client.GetDirectoryInfo(_path);
            if (dir == null)
                Console.WriteLine("Directory not found.");
            else
                dir.PrintListing(Console.Out);
        }
    }
}
