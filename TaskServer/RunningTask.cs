// $Id$
//
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;

namespace TaskServerApplication
{
    sealed class RunningTask : IDisposable
    {
        #region Nested types

        private sealed class AppDomainTaskHost : MarshalByRefObject
        {
            private static log4net.ILog _log = log4net.LogManager.GetLogger(typeof(AppDomainTaskHost));

            public void Run(Guid jobId, string jobDirectory, string dfsJobDirectory, TaskAttemptId taskAttemptId)
            {
                AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

                TaskExecutionUtility.RunTask(jobId, jobDirectory, dfsJobDirectory, taskAttemptId);
            }

            private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
            {
                _log.Fatal("An unexpected error occurred running a task in an AppDomain.", (Exception)e.ExceptionObject);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RunningTask));

        private Process _process;
        private Thread _appDomainThread; // only used when running the task in an appdomain rather than a different process.
        private TaskServer _taskServer;
        private const int _processLaunchRetryCount = 10;

        public event EventHandler ProcessExited;

        public RunningTask(Guid jobId, string jobDirectory, TaskAttemptId taskAttemptId, string dfsJobDirectory, JobConfiguration jobConfiguration, TaskServer taskServer)
        {
            JobId = jobId;
            TaskAttemptId = taskAttemptId;
            FullTaskAttemptId = Job.CreateFullTaskId(jobId, taskAttemptId);
            JobDirectory = jobDirectory;
            DfsJobDirectory = dfsJobDirectory;
            _taskServer = taskServer;
            TaskTimeout = jobConfiguration.GetSetting(TaskServerConfigurationElement.TaskTimeoutJobSettingKey, taskServer.Configuration.TaskServer.TaskTimeout);
        }

        public TaskAttemptStatus State { get; set; }

        public Guid JobId { get; private set; }

        public TaskAttemptId TaskAttemptId { get; private set; }

        public string JobDirectory { get; private set; }

        public string FullTaskAttemptId { get; private set; }

        public string DfsJobDirectory { get; private set; }

        public int TaskTimeout { get; private set; }

        public int TcpChannelPort { get; set; }

        public DateTime LastProgressTimeUtc { get; set; }

        public bool IsTimedOut
        {
            get
            {
                return State == TaskAttemptStatus.Running && (DateTime.UtcNow - LastProgressTimeUtc).TotalMilliseconds > TaskTimeout;
            }
        }

        public void Run(int createProcessDelay)
        {
            if( Debugger.IsAttached || _taskServer.Configuration.TaskServer.RunTaskHostInAppDomain )
            {
                RunTaskAppDomain();
                LastProgressTimeUtc = DateTime.UtcNow;
                State = TaskAttemptStatus.Running;
            }
            else
            {
                _log.DebugFormat("Launching new process for task {0}.", FullTaskAttemptId);
                int retriesLeft = _processLaunchRetryCount;
                bool success = false;
                do
                {
                    try
                    {
                        ProcessStartInfo startInfo = new ProcessStartInfo("dotnet", string.Format("TaskHost.dll \"{0}\" \"{1}\" \"{2}\" \"{3}\" {4}", JobId, JobDirectory, TaskAttemptId.TaskId, DfsJobDirectory, TaskAttemptId.Attempt));
                        startInfo.UseShellExecute = false;
                        startInfo.CreateNoWindow = true;
                        //string profileOutputFile = null;
                        //RuntimeEnvironment.ModifyProcessStartInfo(startInfo, profileOutputFile, null);
                        startInfo.WorkingDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                        _process = new Process();
                        _process.StartInfo = startInfo;
                        _process.EnableRaisingEvents = true;
                        _process.Exited += new EventHandler(_process_Exited);
                        _process.Start();
                        _log.DebugFormat("Host process for task {0} has started, pid = {1}.", FullTaskAttemptId, _process.Id);
                        if( createProcessDelay > 0 )
                        {
                            _log.DebugFormat("Sleeping for {0}ms", createProcessDelay);
                            Thread.Sleep(createProcessDelay);
                        }
                        success = true;
                    }
                    catch( Win32Exception ex )
                    {
                        --retriesLeft;
                        _log.Error(string.Format("Could not create host process for task {0}, {1} retries left.", FullTaskAttemptId, retriesLeft), ex);
                        Thread.Sleep(1000);
                    }
                } while( !success && retriesLeft > 0 );

                if( success )
                {
                    LastProgressTimeUtc = DateTime.UtcNow;
                    State = TaskAttemptStatus.Running;
                }
                else
                    OnProcessExited(EventArgs.Empty);
            }
        }

        public void Kill()
        {
            _log.WarnFormat("Killing task {0}.", FullTaskAttemptId);
            try
            {
                if( Debugger.IsAttached )
                    _appDomainThread.Abort();
                else
                    _process.Kill();
            }
            catch( InvalidOperationException ex )
            {
                _log.Error("Could not kill task.", ex);
            }
            catch( Win32Exception ex )
            {
                _log.Error("Could not kill task.", ex);
            }
        }

        private void OnProcessExited(EventArgs e)
        {
            EventHandler handler = ProcessExited;
            if( handler != null )
                handler(this, e);
        }

        private void _process_Exited(object sender, EventArgs e)
        {
            OnProcessExited(EventArgs.Empty);
        }

        private void RunTaskAppDomain()
        {
            _log.DebugFormat("Running task {0} in an AppDomain.", FullTaskAttemptId);
            _appDomainThread = new Thread(RunTaskAppDomainThread);
            _appDomainThread.Name = FullTaskAttemptId;
            _appDomainThread.IsBackground = true;
            _appDomainThread.Start();
        }

        private void RunTaskAppDomainThread()
        {
            TypeReference.ResolveTypes = true;
            try
            {
                TaskExecutionUtility.RunTask(JobId, JobDirectory, DfsJobDirectory, TaskAttemptId, true);
            }
            catch( Exception ex )
            {
                _log.Error(string.Format("Error running task {0} in app domain", FullTaskAttemptId), ex);
                _taskServer.ReportError(JobId, TaskAttemptId, ex.ToString());
            }

            OnProcessExited(EventArgs.Empty);
        }

        #region IDisposable Members

        public void Dispose()
        {
            if( _process != null )
            {
                _process.Dispose();
                _process = null;
            }
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
