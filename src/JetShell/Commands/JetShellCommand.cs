// Copyright (c) Sven Groot (Ookii.org)
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Jet;

namespace JetShell.Commands;

abstract class JetShellCommand : ICommand
{
    private readonly JetClient _jetClient = new JetClient();

    public JetClient JetClient
    {
        get { return _jetClient; }
    }

    public abstract int Run();
}
