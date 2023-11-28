// Copyright (c) Sven Groot (Ookii.org)
using System.Configuration;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Provides configuration information for the TCP channel.
/// </summary>
public class TcpChannelConfigurationElement : ConfigurationElement
{
    /// <summary>
    /// Gets or sets the size of the spill buffer used by the TCP output channel to collect records before sending them to the receiving stage.
    /// </summary>
    /// <value>The size of the spill buffer. The default value is 20MB.</value>
    [ConfigurationProperty("spillBufferSize", DefaultValue = "20MB", IsRequired = false, IsKey = false)]
    public BinarySize SpillBufferSize
    {
        get { return (BinarySize)this["spillBufferSize"]; }
        set { this["spillBufferSize"] = value; }
    }

    /// <summary>
    /// Gets or sets the usage limit of the spill buffer, between 0 and 1, which triggers a spill.
    /// </summary>
    /// <value>The spill buffer limit, between 0 and 1. The default value is 0.6.</value>
    [ConfigurationProperty("spillBufferLimit", DefaultValue = 0.6f, IsRequired = false, IsKey = false)]
    public float SpillBufferLimit
    {
        get { return (float)this["spillBufferLimit"]; }
        set { this["spillBufferLimit"] = value; }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the TCP output channel keeps the connections to the receiving stage open.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if the connections are kept open; <see langword="false"/> if a new connection is made for each segment.
    /// 	The default value is <see langword="false"/>.
    /// </value>
    [ConfigurationProperty("reuseConnections", DefaultValue = false, IsRequired = false, IsKey = false)]
    public bool ReuseConnections
    {
        get { return (bool)this["reuseConnections"]; }
        set { this["reuseConnections"] = value; }
    }
}
