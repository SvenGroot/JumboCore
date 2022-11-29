// Copyright (c) Sven Groot (Ookii.org)
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [Command("mkdir"), Description("Creates a new directory on the DFS.")]
    class CreateDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public CreateDirectoryCommand([Description("The path of the new directory to create."), ArgumentName("Path")] string path)
        {
            _path = path;
        }

        public override int Run()
        {
            Client.CreateDirectory(_path);
            return 0;
        }
    }
}
