// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;

namespace DfsShell.Commands
{
    [ShellCommand("mv"), Description("Moves a file or directory on the DFS.")]
    class MoveCommand : DfsShellCommand
    {
        private readonly string _sourcePath;
        private readonly string _destinationPath;

        public MoveCommand([Description("The path of the file or directory on the DFS to move."), ArgumentName("Path")] string source,
                           [Description("The path on the DFS to move the file or directory to."), ArgumentName("Destination")] string destination)
        {
            if( source == null )
                throw new ArgumentNullException(nameof(source));
            if( destination == null )
                throw new ArgumentNullException(nameof(destination));

            _sourcePath = source;
            _destinationPath = destination;
        }

        public override void Run()
        {
            Client.Move(_sourcePath, _destinationPath);
        }
    }
}
