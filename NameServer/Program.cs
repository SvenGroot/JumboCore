// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;

namespace NameServerApplication
{
    /// <summary>
    /// Contains the entry point for the NameServer.
    /// </summary>
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("format", StringComparison.OrdinalIgnoreCase))
            {
                log4net.Config.BasicConfigurator.Configure(log4net.LogManager.GetRepository(System.Reflection.Assembly.GetEntryAssembly()));
                FileSystem.Format(DfsConfiguration.GetConfiguration());
            }
            else
            {
                JumboConfiguration.GetConfiguration().Log.ConfigureLogger();
                AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
                System.Threading.Thread.CurrentThread.Name = "main";

                NameServer.Run();

                Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
                Thread.Sleep(Timeout.Infinite);
            }
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            NameServer.Shutdown();
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
