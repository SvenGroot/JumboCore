// Copyright (c) Sven Groot (Ookii.org)
using System.Configuration;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides configuration that may be shared between the Dfs and Jumbo Jet.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Jumbo's shared configuration is specified in the file <c>common.config</c>.
    /// </para>
    /// </remarks>
    /// <seealso href="CommonConfiguration.html">common.config XML documentation</seealso>
    public class JumboConfiguration : ConfigurationSection
    {
        /// <summary>
        /// Gets the configuration element for the network topology support.
        /// </summary>
        /// <value>
        /// The configuration element for the network topology support.
        /// </value>
        [ConfigurationProperty("networkTopology", IsRequired = false, IsKey = false)]
        public Topology.NetworkTopologyConfigurationElement NetworkTopology
        {
            get { return (Topology.NetworkTopologyConfigurationElement)this["networkTopology"]; }
        }

        /// <summary>
        /// Gets the configuration element that configures topology support using the <see cref="Ookii.Jumbo.Topology.PatternTopologyResolver" /> class.
        /// </summary>
        /// <value>
        /// The configuration element that configures topology support using the <see cref="Ookii.Jumbo.Topology.PatternTopologyResolver" /> class.
        /// </value>
        [ConfigurationProperty("patternTopologyResolver", IsRequired = false, IsKey = false)]
        public Topology.PatternTopologyResolverConfigurationElement PatternTopologyResolver
        {
            get { return (Topology.PatternTopologyResolverConfigurationElement)this["patternTopologyResolver"]; }
        }

        /// <summary>
        /// Gets the configuration element that controls global logging settings.
        /// </summary>
        /// <value>The configuration element that controls global logging settings.</value>
        [ConfigurationProperty("log", IsRequired = false, IsKey = false)]
        public LogConfigurationElement Log
        {
            get { return (LogConfigurationElement)this["log"]; }
        }

        /// <summary>
        /// Loads the Jumbo configuration from the application configuration file.
        /// </summary>
        /// <returns>A <see cref="JumboConfiguration"/> object representing the settings in the application configuration file, or
        /// a default instance if the section was not present in the configuration file.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public static JumboConfiguration GetConfiguration()
        {
            var config = (JumboConfiguration)ConfigurationManager.GetSection("ookii.jumbo");
            return config ?? new JumboConfiguration();
        }
    }
}
