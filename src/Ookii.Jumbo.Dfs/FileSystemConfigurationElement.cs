using System;
using System.Configuration;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Provides configuration information about the file system.
/// </summary>
public class FileSystemConfigurationElement : ConfigurationElement
{
    /// <summary>
    /// Gets or sets the URL of the file system.
    /// </summary>
    /// <value>
    /// The URL of the file system.
    /// </value>
    /// <remarks>
    /// <para>
    ///   What type of file system is used is determined by the URL's scheme. The scheme "jdfs://" is used for the Jumbo DFS,
    ///   and "file://" is used for the local file system.
    /// </para>
    /// </remarks>
    [ConfigurationProperty("url", DefaultValue = "jdfs://localhost:9000", IsRequired = false, IsKey = false)]
    public Uri Url
    {
        get { return (Uri)this["url"]; }
        set { this["url"] = value; }
    }
}
