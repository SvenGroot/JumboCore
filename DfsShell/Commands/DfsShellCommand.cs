// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.CommandLine;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands
{
    abstract class DfsShellCommand : ShellCommand
    {
        private readonly FileSystemClient _client = FileSystemClient.Create();

        public FileSystemClient Client
        {
            get { return _client; }
        }
    }
}
