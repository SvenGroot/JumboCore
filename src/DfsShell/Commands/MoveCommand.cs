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
        private readonly string _sourcePath;
        private readonly string _destinationPath;

        public MoveCommand([Description("The path of the file or directory on the DFS to move."), ArgumentName("Path")] string source,
                           [Description("The path on the DFS to move the file or directory to."), ArgumentName("Destination")] string destination)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(destination);

            _sourcePath = source;
            _destinationPath = destination;
        }

        public override int Run()
        {
            Client.Move(_sourcePath, _destinationPath);
            return 0;
        }
    }
}
