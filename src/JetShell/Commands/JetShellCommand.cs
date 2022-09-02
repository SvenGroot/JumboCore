// Copyright (c) Sven Groot (Ookii.org)
using Ookii.CommandLine;
using Ookii.Jumbo.Jet;

namespace JetShell.Commands
{
    abstract class JetShellCommand : ShellCommand
    {
        private readonly JetClient _jetClient = new JetClient();

        public JetClient JetClient
        {
            get { return _jetClient; }
        }
    }
}
