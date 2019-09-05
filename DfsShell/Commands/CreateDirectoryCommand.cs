﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace DfsShell.Commands
{
    [ShellCommand("mkdir"), Description("Creates a new directory on the DFS.")]
    class CreateDirectoryCommand : DfsShellCommand
    {
        private readonly string _path;

        public CreateDirectoryCommand([Description("The path of the new directory to create."), ArgumentName("Path")]string path)
        {
            _path = path;
        }

        public override void Run()
        {
            Client.CreateDirectory(_path);
        }
    }
}
