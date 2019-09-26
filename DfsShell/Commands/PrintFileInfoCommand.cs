// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    [ShellCommand("fileinfo"), Description("Prints information about the specified file.")]
    class PrintFileInfoCommand : DfsShellCommand
    {
        private readonly string _path;

        public PrintFileInfoCommand([Description("The path of the file on the DFS."), ArgumentName("Path")] string path)
        {
            if( path == null )
                throw new ArgumentNullException(nameof(path));

            _path = path;
        }

        public override void Run()
        {
            JumboFile file = Client.GetFileInfo(_path);
            if( file == null )
                Console.WriteLine("File not found.");
            else
                file.PrintFileInfo(Console.Out);
        }
    }
}
