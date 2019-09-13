// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("ls"), Description("Displays the contents of the specified DFS directory.")]
    class ListDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public ListDirectoryCommand([Description("The path of the DFS directory. The default value is /."), ArgumentName("Path")] string path = "/")
        {
            if( path == null )
                throw new ArgumentNullException("path");
            _path = path;
        }

        public override void Run()
        {
            JumboDirectory dir = Client.GetDirectoryInfo(_path);
            if( dir == null )
                Console.WriteLine("Directory not found.");
            else
                dir.PrintListing(Console.Out);
        }
    }
}
