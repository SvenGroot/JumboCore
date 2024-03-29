﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Threading;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;

namespace TaskServerApplication;

sealed class RunningTask : IDisposable
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RunningTask));

    private Process _process;
    private readonly TaskServer _taskServer;
    private const int _processLaunchRetryCount = 10;
    private bool _disposed;

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
        _log.DebugFormat("Launching new process for task {0}.", FullTaskAttemptId);
        var retriesLeft = _processLaunchRetryCount;
        var success = false;
        do
        {
            try
            {
                var startInfo = new ProcessStartInfo("dotnet", string.Format(CultureInfo.InvariantCulture, "TaskHost.dll \"{0}\" \"{1}\" \"{2}\" \"{3}\" {4}", JobId, JobDirectory, TaskAttemptId.TaskId, DfsJobDirectory, TaskAttemptId.Attempt));
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
                if (createProcessDelay > 0)
                {
                    _log.DebugFormat("Sleeping for {0}ms", createProcessDelay);
                    Thread.Sleep(createProcessDelay);
                }
                success = true;
            }
            catch (Win32Exception ex)
            {
                --retriesLeft;
                _log.Error(string.Format(CultureInfo.InvariantCulture, "Could not create host process for task {0}, {1} retries left.", FullTaskAttemptId, retriesLeft), ex);
                Thread.Sleep(1000);
            }
        } while (!success && retriesLeft > 0);

        if (success)
        {
            LastProgressTimeUtc = DateTime.UtcNow;
            State = TaskAttemptStatus.Running;
        }
        else
        {
            OnProcessExited(EventArgs.Empty);
        }
    }

    public void Kill()
    {
        _log.WarnFormat("Killing task {0}.", FullTaskAttemptId);
        try
        {
            _process.Kill();
        }
        catch (InvalidOperationException ex)
        {
            _log.Error("Could not kill task.", ex);
        }
        catch (Win32Exception ex)
        {
            _log.Error("Could not kill task.", ex);
        }
    }

    private void OnProcessExited(EventArgs e)
    {
        var handler = ProcessExited;
        if (handler != null)
        {
            handler(this, e);
        }
    }

    private void _process_Exited(object sender, EventArgs e)
    {
        if (!_disposed)
        {
            OnProcessExited(EventArgs.Empty);
        }
    }

    #region IDisposable Members

    public void Dispose()
    {
        _disposed = true;
        if (_process != null)
        {
            _process.Dispose();
            _process = null;
        }
        GC.SuppressFinalize(this);
    }

    #endregion
}
