﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Configuration;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Provides configuration for the distributed file system.
/// </summary>
/// <remarks>
/// <para>
///   Jumbo's DFS configuration is specified in the file <c>dfs.config</c>.
/// </para>
/// </remarks>
/// <seealso href="DfsConfiguration.html">dfs.config XML documentation</seealso>
public class DfsConfiguration : ConfigurationSection
{
    /// <summary>
    /// Gets the configuration element that configures the file system.
    /// </summary>
    /// <value>
    /// The configuration element for the file system.
    /// </value>
    [ConfigurationProperty("fileSystem", IsRequired = false, IsKey = false)]
    public FileSystemConfigurationElement FileSystem
    {
        get { return (FileSystemConfigurationElement)this["fileSystem"]; }
    }

    /// <summary>
    /// Gets the configuration element that configures the name server.
    /// </summary>
    /// <value>
    /// The configuration element that configures the name server.
    /// </value>
    [ConfigurationProperty("nameServer", IsRequired = false, IsKey = false)]
    public NameServerConfigurationElement NameServer
    {
        get { return (NameServerConfigurationElement)this["nameServer"]; }
    }

    /// <summary>
    /// Gets the configuration element that configures the data server.
    /// </summary>
    /// <value>
    /// The configuration element that configures the data server.
    /// </value>
    [ConfigurationProperty("dataServer", IsRequired = false, IsKey = false)]
    public DataServerConfigurationElement DataServer
    {
        get { return (DataServerConfigurationElement)this["dataServer"]; }
    }

    /// <summary>
    /// Gets the configuration element for the checksums used by both the data servers and the clients.
    /// </summary>
    /// <value>
    /// The configuration element for the checksums used by both the data servers and the clients.
    /// </value>
    [ConfigurationProperty("checksum", IsRequired = false, IsKey = false)]
    public ChecksumConfigurationElement Checksum
    {
        get { return (ChecksumConfigurationElement)this["checksum"]; }
    }

    /// <summary>
    /// Loads the DFS configuration from the application configuration file.
    /// </summary>
    /// <returns>A <see cref="DfsConfiguration"/> object representing the settings in the application configuration file, or
    /// a default instance if the section was not present in the configuration file.</returns>
    public static DfsConfiguration GetConfiguration()
    {
        var config = (DfsConfiguration)ConfigurationManager.GetSection("ookii.jumbo.dfs");
        return config ?? new DfsConfiguration();
    }

    /// <summary>
    /// Loads the DFS configuration from the specified configuration.
    /// </summary>
    /// <param name="configuration">The configuration.</param>
    /// <returns>
    /// A <see cref="DfsConfiguration" /> object representing the settings in the application configuration file, or
    /// a default instance if the section was not present in the configuration file.
    /// </returns>
    public static DfsConfiguration GetConfiguration(Configuration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var config = (DfsConfiguration)configuration.GetSection("ookii.jumbo.dfs");
        return config ?? new DfsConfiguration();
    }
}
