// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Topology;

namespace JobServerApplication;

/// <summary>
/// Information about a task server. This class is safe to access without locking, except for the <see cref="SchedulerInfo"/> property
/// which may only be accessed inside the scheduler lock.
/// </summary>
sealed class TaskServerInfo : TopologyNode
{
    private readonly TaskServerSchedulerInfo _schedulerInfo;
    private long _lastContactUtcTicks;

    public TaskServerInfo(ServerAddress address)
        : base(address)
    {
        _schedulerInfo = new TaskServerSchedulerInfo(this);
    }

    public bool HasReportedStatus { get; set; }

    // Atomicity of setting int values is guaranteed by ECMA spec; no locking needed since we never increment etc. those values, we always outright replcae them
    public int TaskSlots { get; set; }
    public int FileServerPort { get; set; }

    // Setting a DateTime isn't atomic so we keep the value as a long so we can use Interlocked.Exchange to make it atomic.
    public DateTime LastContactUtc
    {
        get { return new DateTime(Interlocked.Read(ref _lastContactUtcTicks), DateTimeKind.Utc); }
        set
        {
            // Atomic update of the last contact time.
            Interlocked.Exchange(ref _lastContactUtcTicks, value.Ticks);
        }
    }

    // Do not access except inside the scheduler lock.
    public TaskServerSchedulerInfo SchedulerInfo
    {
        get { return _schedulerInfo; }
    }

    public bool IsActive
    {
        // Don't schedule tasks on servers that haven't reported for a while
        get { return HasReportedStatus && (DateTime.UtcNow - LastContactUtc).TotalMilliseconds < JobServer.Instance.Configuration.JobServer.TaskServerSoftTimeout; }
    }
}
