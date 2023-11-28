// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet;

namespace JobServerApplication;

/// <summary>
/// Information about a job that can be modified by the scheduler. Only access the properties of this class inside the scheduler lock!
/// </summary>
/// <remarks>
/// Some of these properties have read-only equivalents in <see cref="JobInfo"/>. Those can be read (but not written) without using the scheduler lock.
/// </remarks>
sealed class JobSchedulerInfo
{
    private readonly Dictionary<ServerAddress, TaskServerJobInfo> _taskServers = new Dictionary<ServerAddress, TaskServerJobInfo>();
    private readonly Dictionary<string, List<TaskInfo>> _rackTasks = new Dictionary<string, List<TaskInfo>>();
    private readonly JobInfo _job;
    private readonly Dictionary<string, JumboFile> _files = new Dictionary<string, JumboFile>();

    public JobSchedulerInfo(JobInfo job)
    {
        _job = job;
    }

    public JobState State { get; set; }

    public int UnscheduledTasks { get; set; }

    public int FinishedTasks { get; set; }

    public int Errors { get; set; }

    public TaskServerJobInfo GetTaskServer(ServerAddress address)
    {
        if (_taskServers.TryGetValue(address, out var server))
        {
            return server;
        }
        else
        {
            return null;
        }
    }

    public void AddTaskServer(TaskServerInfo server)
    {
        if (!_taskServers.ContainsKey(server.Address))
        {
            _taskServers.Add(server.Address, new TaskServerJobInfo(server, _job));
        }
    }

    public int TaskServerCount
    {
        get { return _taskServers.Count; }
    }

    public List<TaskInfo> GetRackTasks(string rackId)
    {
        if (_rackTasks.TryGetValue(rackId, out var tasks))
        {
            return tasks;
        }
        else
        {
            return null;
        }
    }

    public void AddRackTasks(string rackId, List<TaskInfo> tasks)
    {
        ArgumentNullException.ThrowIfNull(tasks);

        _rackTasks.Add(rackId, tasks);
    }

    public IEnumerable<TaskServerJobInfo> TaskServers
    {
        get { return _taskServers.Values; }
    }

    public bool NeedsCleanup
    {
        get { return _taskServers.Values.Any(server => server.NeedsCleanup); }
    }


    public JumboFile GetFileInfo(DfsClient dfsClient, string path)
    {
        if (!_files.TryGetValue(path, out var file))
        {
            file = dfsClient.NameServer.GetFileInfo(path);
            if (file == null)
            {
                throw new ArgumentException("File doesn't exist."); // TODO: Different exception type.
            }

            _files.Add(path, file);
        }
        return file;
    }

    public void AbortTasks()
    {
        foreach (var jobTask in _job.Stages.SelectMany(stage => stage.Tasks))
        {
            if (jobTask.State <= TaskState.Running)
            {
                jobTask.SchedulerInfo.State = TaskState.Aborted;
            }
        }
    }
}
