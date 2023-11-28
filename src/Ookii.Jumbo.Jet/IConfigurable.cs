// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Interface for classes that need the DFS, Jet, and/or Job configuration.
/// </summary>
public interface IConfigurable
{
    /// <summary>
    /// Gets or sets the configuration used to access the Distributed File System.
    /// </summary>
    DfsConfiguration? DfsConfiguration { get; set; }

    /// <summary>
    /// Gets or sets the configuration used to access the Jet servers.
    /// </summary>
    JetConfiguration? JetConfiguration { get; set; }

    /// <summary>
    /// Gets or sets the configuration for the task attempt.
    /// </summary>
    TaskContext? TaskContext { get; set; }

    /// <summary>
    /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
    /// after setting the configuration.
    /// </summary>
    void NotifyConfigurationChanged();
}
