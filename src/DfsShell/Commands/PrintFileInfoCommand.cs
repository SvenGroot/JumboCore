// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;

namespace DfsShell.Commands
{
    [Command("fileinfo"), Description("Prints information about the specified file.")]
    class PrintFileInfoCommand : DfsShellCommand
    {
        private readonly string _path;

        public PrintFileInfoCommand([Description("The path of the file on the DFS."), ArgumentName("Path")] string path)
        {
            ArgumentNullException.ThrowIfNull(path);

            _path = path;
        }

        public override int Run()
        {
            var file = Client.GetFileInfo(_path);
            if (file == null)
            {
                Console.WriteLine("File not found.");
                return 1;
            }

            file.PrintFileInfo(Console.Out);
            return 0;
        }
    }
}
