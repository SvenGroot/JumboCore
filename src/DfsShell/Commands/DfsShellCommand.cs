// Copyright (c) Sven Groot (Ookii.org)
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsShell.Commands;

abstract class DfsShellCommand : ICommand
{
    private readonly FileSystemClient _client = FileSystemClient.Create();

    public FileSystemClient Client
    {
        get { return _client; }
    }

    public abstract int Run();
}
