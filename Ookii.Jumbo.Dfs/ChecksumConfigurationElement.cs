// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration for the file checksums used by both the data server and client.
    /// </summary>
    public class ChecksumConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets a value that indicates whether checksums are enabled.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if this checksums are enabled; otherwise, <see langword="false" />.
        /// </value>
        /// <remarks>
        /// If you change this attribute on an existing file system it may prevent you from reading
        /// the existing files.
        /// </remarks>
        [ConfigurationProperty("enabled", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool IsEnabled
        {
            get { return (bool)this["enabled"]; }
            set { this["enabled"] = value; }
        }
    }
}
