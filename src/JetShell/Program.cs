// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Reflection;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Rpc;

namespace JetShell;

static class Program
{
    public static int Main(string[] args)
    {
        AssemblyResolver.Register();

        var repository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
        log4net.Config.XmlConfigurator.Configure(repository,
            new FileInfo(ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath));

        repository.Threshold = log4net.Core.Level.Info;
        var options = new CommandOptions()
        {
            ArgumentNamePrefixes = new[] { "-" }, // DFS paths use / as the directory separator, so use - even on Windows.
        };

        try
        {
            var manager = new GeneratedManager(options);
            return manager.RunCommand(args) ?? 1;
        }
        catch (SocketException ex)
        {
            WriteError("An error occurred communicating with the server:", ex.Message);
        }
        catch (DfsException ex)
        {
            WriteError("An error occurred accessing the distributed file system:", ex.Message);
        }
        catch (IOException ex)
        {
            WriteError("An error occurred executing the command:", ex.Message);
        }
        catch (ArgumentException ex)
        {
            WriteError("An error occurred executing the command:", ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            WriteError("Invalid operation:", ex.Message);
        }
        catch (Exception ex)
        {
            WriteError(null, ex.ToString());
        }

        RpcHelper.CloseConnections();
        return 1;
    }

    private static void WriteError(string errorType, string message)
    {
        using (TextWriter writer = LineWrappingTextWriter.ForConsoleError())
        {
            if (errorType != null)
                writer.WriteLine(errorType);
            writer.WriteLine(message);
        }
    }
}
