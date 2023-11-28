// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Configuration;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Provides configuration for the Jumbo distributed execution environment.
/// </summary>
/// <remarks>
/// <para>
///   Jumbo Jet configuration is specified in the file <c>jet.config</c>.
/// </para>
/// </remarks>
/// <seealso href="JetConfiguration.html">jet.config XML documentation</seealso>
public class JetConfiguration : ConfigurationSection
{
    /// <summary>
    /// Gets configuration for the job server.
    /// </summary>
    [ConfigurationProperty("jobServer", IsRequired = false, IsKey = false)]
    public JobServerConfigurationElement JobServer
    {
        get { return (JobServerConfigurationElement)this["jobServer"]; }
    }

    /// <summary>
    /// Gets configuration for the task server.
    /// </summary>
    [ConfigurationProperty("taskServer", IsRequired = false, IsKey = false)]
    public TaskServerConfigurationElement TaskServer
    {
        get { return (TaskServerConfigurationElement)this["taskServer"]; }
    }

    /// <summary>
    /// Gets configuration for the file channel.
    /// </summary>
    [ConfigurationProperty("fileChannel", IsRequired = false, IsKey = false)]
    public FileChannelConfigurationElement FileChannel
    {
        get { return (FileChannelConfigurationElement)this["fileChannel"]; }
    }

    /// <summary>
    /// Gets the configuration for the TCP channel.
    /// </summary>
    /// <value>The <see cref="TcpChannelConfigurationElement"/> for the TCP channel.</value>
    [ConfigurationProperty("tcpChannel", IsRequired = false, IsKey = false)]
    public TcpChannelConfigurationElement TcpChannel
    {
        get { return (TcpChannelConfigurationElement)this["tcpChannel"]; }
    }

    /// <summary>
    /// Gets configuration for the merge record reader.
    /// </summary>
    /// <value>A <see cref="MergeRecordReaderConfigurationElement"/> with the configuratin for the merge record reader.</value>
    [ConfigurationProperty("mergeRecordReader", IsRequired = false, IsKey = false)]
    public MergeRecordReaderConfigurationElement MergeRecordReader
    {
        get { return (MergeRecordReaderConfigurationElement)this["mergeRecordReader"]; }
    }

    /// <summary>
    /// Loads the Jet configuration from the application configuration file.
    /// </summary>
    /// <returns>A <see cref="JetConfiguration"/> object representing the settings in the application configuration file, or
    /// a default instance if the section was not present in the configuration file.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
    public static JetConfiguration GetConfiguration()
    {
        var config = (JetConfiguration)ConfigurationManager.GetSection("ookii.jumbo.jet");
        return config ?? new JetConfiguration();
    }

    /// <summary>
    /// Loads the Jet configuration from the specified configuratino.
    /// </summary>
    /// <param name="configuration">The configuration.</param>
    /// <returns>
    /// A <see cref="JetConfiguration" /> object representing the settings in the application configuration file, or
    /// a default instance if the section was not present in the configuration file.
    /// </returns>
    public static JetConfiguration GetConfiguration(Configuration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        var config = (JetConfiguration)configuration.GetSection("ookii.jumbo.jet");
        return config ?? new JetConfiguration();
    }
}
