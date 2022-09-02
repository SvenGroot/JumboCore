// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Rpc;

namespace TaskHost
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if (args.Length != 5)
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            var jobId = new Guid(args[0]);
            var jobDirectory = args[1];
            var taskId = args[2];
            var dfsJobDirectory = args[3];
            var attempt = Convert.ToInt32(args[4], CultureInfo.InvariantCulture);
            var taskAttemptId = new TaskAttemptId(new TaskId(taskId), attempt);

            TaskExecutionUtility.RunTask(jobId, jobDirectory, dfsJobDirectory, taskAttemptId);

            RpcHelper.CloseConnections(); // Cleanly close connections helps save server resources

            return 0;
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            _log.Fatal("An unhandled exception occurred.", (Exception)e.ExceptionObject);
        }
    }
}
