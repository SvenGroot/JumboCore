// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Threading;
using Ookii.Jumbo;

namespace DataServerApplication;

static class Program
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));
    private static DataServer _server;
    private static Thread _serverThread;

    private static void Main()
    {
        JumboConfiguration.GetConfiguration().Log.ConfigureLogger();
        AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
        System.Threading.Thread.CurrentThread.Name = "entry";
        //RemotingConfiguration.Configure("DataServer.exe.config", false);
        _serverThread = new Thread(MainThread);
        _serverThread.IsBackground = true;
        _serverThread.Name = "main";
        _serverThread.Start();
        Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
        Thread.Sleep(Timeout.Infinite);
    }

    static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
    {
        _log.Info("---- Data Server shutting down ----");
        _server.Abort();
        _serverThread.Join();
    }

    private static void MainThread()
    {
        _log.Info("---- Data Server is starting ----");
        _log.LogEnvironmentInformation();
        _server = new DataServer();
        _server.Run();
    }

    private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        _log.Fatal("Unhandled exception.", (Exception)e.ExceptionObject);
    }
}
