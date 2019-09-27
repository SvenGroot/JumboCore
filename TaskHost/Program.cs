// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet;
using System.IO;
using System.Reflection;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.IO;
using System.Threading;
using Ookii.Jumbo;
using System.Diagnostics;
using Ookii.Jumbo.Rpc;
using System.Globalization;

namespace TaskHost
{
    static class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(Program));

        public static int Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            if( args.Length != 5 )
            {
                _log.Error("Invalid invocation.");
                return 1;
            }

            Guid jobId = new Guid(args[0]);
            string jobDirectory = args[1];
            string taskId = args[2];
            string dfsJobDirectory = args[3];
            int attempt = Convert.ToInt32(args[4], CultureInfo.InvariantCulture);
            TaskAttemptId taskAttemptId = new TaskAttemptId(new TaskId(taskId), attempt);

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
