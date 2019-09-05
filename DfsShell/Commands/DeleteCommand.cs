// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;

namespace DfsShell.Commands
{
    [ShellCommand("rm"), Description("Deletes a file or directory from the DFS.")]
    class DeleteCommand : DfsShellCommand
    {
        private readonly string _path;

        public DeleteCommand([Description("The path of the file or directory on the DFS to delete."), ArgumentName("Path")] string path)
        {
            if( path == null )
                throw new ArgumentNullException("path");

            _path = path;
        }

        [CommandLineArgument, Description("Recursively delete all children of a directory.")]
        public bool Recursive { get; set; }

        public override void Run()
        {
            if( !Client.Delete(_path, Recursive) )
                Console.Error.WriteLine("Path did not exist.");
        }
    }
}
