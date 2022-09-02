// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Provides a simple topology resolver that uses regular expressions or range expressions to determine which rack each node belongs to.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   All pattern comparisons are case-insensitive.
    /// </para>
    /// </remarks>
    public sealed class PatternTopologyResolver : ITopologyResolver
    {
        #region Nested types

        private class RackInfo
        {
            public string RackId { get; set; }
            public Regex Regex { get; set; }
            public RangeExpression RangeExpression { get; set; }
        }

        #endregion

        private readonly List<RackInfo> _racks = new List<RackInfo>();

        /// <summary>
        /// Initializes a new instance of the <see cref="PatternTopologyResolver"/> class.
        /// </summary>
        /// <param name="configuration">The jumbo configuration to use. May be <see langword="null"/>.</param>
        public PatternTopologyResolver(JumboConfiguration configuration)
        {
            if (configuration == null)
                configuration = JumboConfiguration.GetConfiguration();

            foreach (RackConfigurationElement rackConfig in configuration.PatternTopologyResolver.Racks)
            {
                var rack = new RackInfo() { RackId = rackConfig.RackId };
                switch (configuration.PatternTopologyResolver.PatternType)
                {
                case PatternType.RegularExpression:
                    rack.Regex = new Regex(rackConfig.Pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                    break;
                case PatternType.RangeExpression:
                    rack.RangeExpression = new RangeExpression(rackConfig.Pattern);
                    break;
                default:
                    throw new InvalidOperationException("Unknown pattern type.");
                }
                _racks.Add(rack);
            }
        }

        #region ITopologyResolver Members

        /// <summary>
        /// Determines which rack a node belongs to.
        /// </summary>
        /// <param name="hostName">The host name of the node.</param>
        /// <returns>The rack ID of the rack that the server belongs to.</returns>
        public string ResolveNode(string hostName)
        {
            ArgumentNullException.ThrowIfNull(hostName);

            foreach (var rack in _racks)
            {
                bool match;
                if (rack.Regex != null)
                    match = rack.Regex.IsMatch(hostName);
                else
                    match = rack.RangeExpression.Match(hostName, false);

                if (match)
                    return rack.RackId;
            }

            return null;
        }

        #endregion
    }
}
