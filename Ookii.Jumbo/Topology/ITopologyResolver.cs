// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Interface for network topology resolvers.
    /// </summary>
    public interface ITopologyResolver
    {
        /// <summary>
        /// Determines which rack a node belongs to.
        /// </summary>
        /// <param name="hostName">The host name of the node.</param>
        /// <returns>The rack ID of the rack that the server belongs to.</returns>
        string ResolveNode(string hostName);
    }
}
