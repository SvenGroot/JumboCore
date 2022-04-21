// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Provides configuration for a rack for the <see cref="PatternTopologyResolver"/>.
    /// </summary>
    public class RackConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the ID of the rack.
        /// </summary>
        [ConfigurationProperty("id", IsRequired = true, IsKey = true)]
        public string RackId
        {
            get { return (string)this["id"]; }
            set { this["id"] = value; }
        }

        /// <summary>
        /// Gets or sets the pattern used to identify nodes of this rack.
        /// </summary>
        [ConfigurationProperty("pattern", IsRequired = true, IsKey = false)]
        public string Pattern
        {
            get { return (string)this["pattern"]; }
            set { this["pattern"] = value; }
        }
    }
}
