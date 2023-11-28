// Copyright (c) Sven Groot (Ookii.org)
using System.Configuration;

namespace Ookii.Jumbo.Topology;

/// <summary>
/// Provides configuration for the network topology support.
/// </summary>
public class NetworkTopologyConfigurationElement : ConfigurationElement
{
    /// <summary>
    /// Gets or sets the type name of the resolver to use.
    /// </summary>
    [ConfigurationProperty("resolver", DefaultValue = "Ookii.Jumbo.Topology.PatternTopologyResolver, Ookii.Jumbo", IsRequired = false, IsKey = false)]
    public string Resolver
    {
        get { return (string)this["resolver"]; }
        set { this["resolver"] = value; }
    }
}
