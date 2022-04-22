// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Threading;
using Ookii.Jumbo;

namespace JobServerApplication
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        public static void Main()
        {
            JumboConfiguration.GetConfiguration().Log.ConfigureLogger();
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

            Thread.CurrentThread.Name = "main";
            JobServer.Run();

            Thread.Sleep(Timeout.Infinite);
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            JobServer.Shutdown();
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
