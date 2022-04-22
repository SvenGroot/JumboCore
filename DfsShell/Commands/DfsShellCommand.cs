// Copyright (c) Sven Groot (Ookii.org)
using Ookii.CommandLine;
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
