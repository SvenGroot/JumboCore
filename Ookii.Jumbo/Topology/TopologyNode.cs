// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Base class for cluster nodes that are part of a network topology.
    /// </summary>
    public class TopologyNode
    {
        private readonly ServerAddress _address;

        /// <summary>
        /// Initializes a new instance of the <see cref="TopologyNode"/> class.
        /// </summary>
        /// <param name="address">The address of the node.</param>
        public TopologyNode(ServerAddress address)
        {
            if( address == null )
                throw new ArgumentNullException("address");

            _address = address;
        }

        /// <summary>
        /// Gets the address of the node.
        /// </summary>
        public ServerAddress Address
        {
            get { return _address; }
        }

        /// <summary>
        /// Gets the rack this node belongs to.
        /// </summary>
        public Rack Rack { get; internal set; }


        /// <summary>
        /// Gets the distance between a node in the topology and another node (which may or may not be in the topology).
        /// </summary>
        /// <param name="hostName">The host name of the other node. May be <see langword="null"/>.</param>
        /// <param name="rackId">The rack ID of the other node. May be <see langword="null"/>.</param>
        /// <returns>0 if the two nodes are identical, 1 if they are in the same rack, or 2 if they are in different racks.</returns>
        public int DistanceFrom(string hostName, string rackId)
        {
            if( Address.HostName == hostName )
                return 0;
            else if( Rack.RackId == rackId )
                return 1;
            else
                return 2;
        }

        /// <summary>
        /// Gets the distance between a node in the topology and another node.
        /// </summary>
        /// <param name="node">The other node.</param>
        /// <returns>0 if the two nodes are identical, 1 if they are in the same rack, or 2 if they are in different racks.</returns>
        public int DistanceFrom(TopologyNode node)
        {
            if( node == null )
                throw new ArgumentNullException("node");

            return DistanceFrom(node.Address.HostName, node.Rack.RackId);
        }
    }
}
