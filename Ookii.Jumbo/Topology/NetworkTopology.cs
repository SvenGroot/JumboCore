// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Represents a network topology, grouping nodes into racks.
    /// </summary>
    public class NetworkTopology
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NetworkTopology));

        private readonly SortedList<string, Rack> _racks = new SortedList<string, Rack>();
        private readonly ITopologyResolver _resolver;

        /// <summary>
        /// Initializes a new instance of the <see cref="NetworkTopology"/> class.
        /// </summary>
        /// <param name="configuration">The jumbo configuration to use, or <see langword="null"/> to use the application configuration.</param>
        public NetworkTopology(JumboConfiguration configuration)
        {
            if (configuration == null)
                configuration = JumboConfiguration.GetConfiguration();

            _log.InfoFormat("Using topology resolver type {0}.", configuration.NetworkTopology.Resolver);
            _resolver = (ITopologyResolver)Activator.CreateInstance(Type.GetType(configuration.NetworkTopology.Resolver, true), configuration);
        }

        /// <summary>
        /// Gets the racks in the topology.
        /// </summary>
        public IList<Rack> Racks
        {
            get { return _racks.Values; }
        }

        /// <summary>
        /// Adds a node to the topology.
        /// </summary>
        /// <param name="node">The node to add.</param>
        public void AddNode(TopologyNode node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            var rackId = ResolveNode(node.Address.HostName);
            _log.InfoFormat("Node {0} was resolved to rack {1}.", node.Address, rackId);
            if (!_racks.TryGetValue(rackId, out var rack))
            {
                rack = new Rack(rackId);
                _racks.Add(rackId, rack);
            }

            rack.Nodes.Add(node);
        }

        /// <summary>
        /// Removes a node from the topology that it's part of.
        /// </summary>
        /// <param name="node">The node to remove.</param>
        public static void RemoveNode(TopologyNode node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            if (node.Rack == null)
                throw new ArgumentException("The specified node is not part of a rack.");

            node.Rack.Nodes.Remove(node);
        }

        /// <summary>
        /// Determines which rack a node belongs to.
        /// </summary>
        /// <param name="hostName">The host name of the node.</param>
        /// <returns>The rack ID of the rack that the server belongs to.</returns>
        public string ResolveNode(string hostName)
        {
            return _resolver.ResolveNode(hostName) ?? "(default)";
        }

    }
}
