// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Configuration;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Provides configuration information for the task server.
/// </summary>
public class TaskServerConfigurationElement : ConfigurationElement
{
    /// <summary>
    /// The key of the setting in <see cref="Jobs.JobConfiguration.JobSettings"/> used to override the default <see cref="TaskTimeout"/>.
    /// The setting should be an <see cref="Int32"/> indicating the timeout in milliseconds.
    /// </summary>
    public const string TaskTimeoutJobSettingKey = "TaskServer.TaskTimeout";

    /// <summary>
    /// Gets or sets the local directory for task files.
    /// </summary>
    [ConfigurationProperty("taskDirectory", IsRequired = true, IsKey = false)]
    public string TaskDirectory
    {
        get { return (string)this["taskDirectory"]; }
        set { this["taskDirectory"] = value; }
    }

    /// <summary>
    /// Gets or sets the port number on which the task server's RPC server listens.
    /// </summary>
    [ConfigurationProperty("port", DefaultValue = 9501, IsRequired = false, IsKey = false)]
    public int Port
    {
        get { return (int)this["port"]; }
        set { this["port"] = value; }
    }

    /// <summary>
    /// Gets or sets a value that indicates whether the server should listen on both IPv6 and IPv4.
    /// </summary>
    /// <value>
    /// <see langword="true"/> if the server should listen on both IPv6 and IPv4; <see langword="false"/>
    /// if the server should listen only on IPv6 if it's available, and otherwise on IPv4.
    /// </value>
    [ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = true, IsRequired = false, IsKey = false)]
    public bool ListenIPv4AndIPv6
    {
        get { return (bool)this["listenIPv4AndIPv6"]; }
        set { this["listenIPv4AndIPv6"] = value; }
    }

    /// <summary>
    /// Gets or sets the maximum number of tasks to schedule on this server.
    /// </summary>
    [ConfigurationProperty("taskSlots", DefaultValue = 2, IsRequired = false, IsKey = false)]
    public int TaskSlots
    {
        get { return (int)this["taskSlots"]; }
        set { this["taskSlots"] = value; }
    }

    /// <summary>
    /// The port number that the TCP server for file channels listens on.
    /// </summary>
    [ConfigurationProperty("fileServerPort", DefaultValue = 9502, IsRequired = false, IsKey = false)]
    public int FileServerPort
    {
        get { return (int)this["fileServerPort"]; }
        set { this["fileServerPort"] = value; }
    }

    /// <summary>
    /// Gets or sets the maximum number of simultaneous connections allowed to the file channel channel TCP server.
    /// </summary>
    [ConfigurationProperty("fileServerMaxConnections", DefaultValue = 10, IsRequired = false, IsKey = false)]
    [IntegerValidator(MinValue = 1, MaxValue = Int32.MaxValue, ExcludeRange = false)]
    public int FileServerMaxConnections
    {
        get { return (int)this["fileServerMaxConnections"]; }
        set { this["fileServerMaxConnections"] = value; }
    }

    /// <summary>
    /// Gets or sets the maximum number of partition file index entries to keep in the index cache.
    /// </summary>
    /// <value>The file server max index cache size.</value>
    [ConfigurationProperty("fileServerMaxIndexCacheSize", DefaultValue = 25, IsRequired = false, IsKey = false)]
    public int FileServerMaxIndexCacheSize
    {
        get { return (int)this["fileServerMaxIndexCacheSize"]; }
        set { this["fileServerMaxIndexCacheSize"] = value; }
    }

    /// <summary>
    /// Gets or sets the number of milliseconds to wait between creating TaskHost processes.
    /// </summary>
    [ConfigurationProperty("processCreationDelay", DefaultValue = 0, IsRequired = false, IsKey = false)]
    public int ProcessCreationDelay
    {
        get { return (int)this["processCreationDelay"]; }
        set { this["processCreationDelay"] = value; }
    }

    /// <summary>
    /// Gets or sets a value that indicates whether the task should periodically log processor and memory usage status.
    /// </summary>
    [ConfigurationProperty("logSystemStatus", DefaultValue = false, IsRequired = false, IsKey = false)]
    public bool LogSystemStatus
    {
        get { return (bool)this["logSystemStatus"]; }
        set { this["logSystemStatus"] = value; }
    }

    /// <summary>
    /// Gets or sets the progress interval.
    /// </summary>
    /// <value>The progress interval.</value>
    [ConfigurationProperty("progressInterval", DefaultValue = 3000, IsRequired = false, IsKey = false)]
    public int ProgressInterval
    {
        get { return (int)this["progressInterval"]; }
        set { this["progressInterval"] = value; }
    }

    /// <summary>
    /// Gets or sets the heartbeat interval.
    /// </summary>
    /// <value>The heartbeat interval.</value>
    [ConfigurationProperty("heartbeatInterval", DefaultValue = 3000, IsRequired = false, IsKey = false)]
    public int HeartbeatInterval
    {
        get { return (int)this["heartbeatInterval"]; }
        set { this["heartbeatInterval"] = value; }
    }

    /// <summary>
    /// Gets or sets the timeout, in milliseconds, after which a task is declared dead if it hasn't reported progress.
    /// </summary>
    [ConfigurationProperty("taskTimeout", DefaultValue = 600000, IsRequired = false, IsKey = false)]
    public int TaskTimeout
    {
        get { return (int)this["taskTimeout"]; }
        set { this["taskTimeout"] = value; }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the task server immediately notifies the job server when a task completes.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if the task server immediately notifies the job server of a completed task; <see langword="false"/> to wait for the next heartbeat instead.
    /// </value>
    [ConfigurationProperty("immediateCompletedTaskNotification", DefaultValue = true, IsRequired = false, IsKey = false)]
    public bool ImmediateCompletedTaskNotification
    {
        get { return (bool)this["immediateCompletedTaskNotification"]; }
        set { this["immediateCompletedTaskNotification"] = value; }
    }
}
