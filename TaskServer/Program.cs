// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Ookii.Jumbo;

namespace TaskServerApplication
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        private static Thread _mainThread;

        static void Main(string[] args)
        {
            JumboConfiguration.GetConfiguration().Log.ConfigureLogger();
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
            _mainThread = Thread.CurrentThread;
            Thread.CurrentThread.Name = "main";
            TaskServer.Run();
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            TaskServer.Shutdown();
            _mainThread.Join();
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
        }
    }
}
