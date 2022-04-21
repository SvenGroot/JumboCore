// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Provides configuration for the <see cref="PatternTopologyResolver"/>.
    /// </summary>
    public class PatternTopologyResolverConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the type of the patterns used to assign nodes to racks.
        /// </summary>
        /// <value>One of the values of the <see cref="PatternType"/> enumeration.</value>
        [ConfigurationProperty("patternType", DefaultValue = PatternType.RegularExpression, IsRequired = false, IsKey = false)]
        public PatternType PatternType
        {
            get { return (PatternType)this["patternType"]; }
            set { this["patternType"] = value; }
        }

        /// <summary>
        /// Gets the racks of this configuration element.
        /// </summary>
        [ConfigurationProperty("racks", IsRequired = true, IsKey = false)]
        public RackConfigurationElementCollection Racks
        {
            get { return (RackConfigurationElementCollection)this["racks"]; }
        }
    }
}
