// $Id$
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using System.Threading;
using Ookii.Jumbo.Rpc;
using Ookii.CommandLine;
using JetShell.Commands;
using Ookii.Jumbo.Dfs;
using System.Configuration;

namespace JetShell
{
    static class Program
    {
        public static int Main(string[] args)
        {
            AssemblyResolver.Register();

            var repository = log4net.LogManager.GetRepository(Assembly.GetEntryAssembly());
            log4net.Config.XmlConfigurator.Configure(repository,
                new FileInfo(ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None).FilePath));

            repository.Threshold = log4net.Core.Level.Info;
            CreateShellCommandOptions options = new CreateShellCommandOptions()
            {                
                ArgumentNamePrefixes = new[] { "-" }, // DFS paths use / as the directory separator, so use - even on Windows.
                CommandDescriptionFormat = "    {0}\n{1}\n",
                CommandDescriptionIndent = 8,
                UsageOptions = new WriteUsageOptions()
                {
                    UsagePrefix = "Usage: JetShell",
                    ArgumentDescriptionFormat = "    {3}{0} {2}\n{1}\n",
                    ArgumentDescriptionIndent = 8
                }
            };

            try
            {
                return ShellCommand.RunShellCommand(Assembly.GetExecutingAssembly(), args, 0, options);
            }
            catch( SocketException ex )
            {
                WriteError("An error occurred communicating with the server:", ex.Message);
            }
            catch( DfsException ex )
            {
                WriteError("An error occurred accessing the distributed file system:", ex.Message);
            }
            catch( IOException ex )
            {
                WriteError("An error occurred executing the command:", ex.Message);
            }
            catch( ArgumentException ex )
            {
                WriteError("An error occurred executing the command:", ex.Message);
            }
            catch( InvalidOperationException ex )
            {
                WriteError("Invalid operation:", ex.Message);
            }
            catch( Exception ex )
            {
                WriteError(null, ex.ToString());
            }

            RpcHelper.CloseConnections();
            return 1;
        }

        private static void WriteError(string errorType, string message)
        {
            using( TextWriter writer = LineWrappingTextWriter.ForConsoleError() )
            {
                if( errorType != null )
                    writer.WriteLine(errorType);
                writer.WriteLine(message);
            }
        }
    }
}
