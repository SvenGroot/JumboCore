// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Topology;
using Ookii.Jumbo.Dfs;

namespace NameServerApplication
{
    sealed class ReplicaPlacement
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(ReplicaPlacement));
        private readonly Random _random = new Random();

        private readonly NetworkTopology _topology;
        private readonly DfsConfiguration _configuration;

        public ReplicaPlacement(DfsConfiguration configuration, NetworkTopology topology)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");
            if( topology == null )
                throw new ArgumentNullException("topology");

            _configuration = configuration;
            _topology = topology;
        }

        public BlockAssignment AssignBlockToDataServers(IEnumerable<DataServerInfo> dataServers, BlockInfo block, string writerHostName, bool useLocalReplica)
        {
            long freeSpaceThreshold = (long)_configuration.NameServer.DataServerFreeSpaceThreshold;
            Guid blockId = block.BlockId;
            bool forceDifferentRack = false;
            var currentDataServers = (from server in dataServers
                                      where server.Blocks.Contains(blockId)
                                      select server).ToList();
            var eligibleServers = (from server in dataServers
                                   where server.HasReportedBlocks && !server.Blocks.Contains(blockId) && server.DiskSpaceFree >= freeSpaceThreshold
                                   select server).ToList();

            int serversNeeded = block.File.ReplicationFactor - currentDataServers.Count;
            int serversUsed = currentDataServers.Count;

            List<DataServerInfo> newDataServers = new List<DataServerInfo>(serversNeeded);

            string writerRackId = null;
            if( currentDataServers.Count > 1 && _topology.Racks.Count > 1 )
            {
                // If there is more than one current replica, we need to determine if they're on different racks.
                // If they're not we need to force the next replica to be on a different rack.
                forceDifferentRack = (from server in currentDataServers select server.Rack.RackId).Distinct().Count() == 1;
            }

            string originalWriterHostName = writerHostName;
            if( writerHostName != null )
            {
                if( currentDataServers.Count > 0 )
                    throw new ArgumentException("Cannot specify a writer for a re-replication block assignment.", "writerHostName");
                writerRackId = _topology.ResolveNode(writerHostName);
            }
            else
            {
                if( currentDataServers.Count == 0 )
                    throw new ArgumentException("No writer specified for a first replication.", "writerHostName");
                writerHostName = currentDataServers[0].Address.HostName;
                writerRackId = currentDataServers[0].Rack.RackId;
            }

            while( serversNeeded > 0 )
            {
                DataServerInfo selectedServer;
                switch( serversUsed )
                {
                case 0:
                    // This is the first replica. Try to place it on the same node as the write if possible, and if useLocalReplica is true.
                    // TODO: If the writer is not in the cluster at all, this will favour the default rack if there is one.
                    // If useLocalReplica is false, we don't pass the host name so all nodes in the rack are treated equal (including the local one)
                    selectedServer = SelectClosestServerWithMinimumDistance(eligibleServers, useLocalReplica ? writerHostName : null, writerRackId, 0);
                    // For the next replicas, these will be used to place it in a different or the same rack as the first replica.
                    writerHostName = selectedServer.Address.HostName;
                    writerRackId = selectedServer.Rack.RackId;
                    break;
                case 1:
                    // The second replica should go on a different rack than the first.
                    if( _topology.Racks.Count == 1 )
                        selectedServer = SelectRandomServer(eligibleServers);
                    else
                        selectedServer = SelectClosestServerWithMinimumDistance(eligibleServers, writerHostName, writerRackId, 2);
                    break;
                case 2:
                    // The third replica should go on the same rack as the first, unless forceDifferentRack is true.
                    // TODO: If there are no more eligible nodes in the same rack, this would cause random placement, and we might want to try matching the second rack if possible
                    if( _topology.Racks.Count == 1 )
                        selectedServer = SelectRandomServer(eligibleServers);
                    else
                    {
                        selectedServer = SelectClosestServerWithMinimumDistance(eligibleServers, writerHostName, writerRackId, forceDifferentRack ? 2 : 1);
                        forceDifferentRack = false;
                    }
                    break;
                default:
                    if( _topology.Racks.Count > 1 && forceDifferentRack )
                    {
                        selectedServer = SelectClosestServerWithMinimumDistance(eligibleServers, writerHostName, writerRackId, 2);
                        forceDifferentRack = false;
                    }
                    else
                        selectedServer = SelectRandomServer(eligibleServers);
                    break;
                }

                eligibleServers.Remove(selectedServer);
                // If useLocalReplica is false, the local node can still be selected randomly. If that's the case, we want to make sure it's at the front.
                if( !useLocalReplica && originalWriterHostName == selectedServer.Address.HostName )
                    newDataServers.Insert(0, selectedServer);
                else
                    newDataServers.Add(selectedServer);
                selectedServer.PendingBlocks.Add(block.BlockId);
                --serversNeeded;
                ++serversUsed;
            }

            for( int i = 0; i < newDataServers.Count - 1; ++i )
            {
                int closestNodeIndex = i + 1;
                int distance = newDataServers[i].DistanceFrom(newDataServers[closestNodeIndex]);
                // This uses the fact that the distance if never greater than 2, and that a distance 0 won't happen outside of test scenarios.
                // If the next node in line is in the same rack, there's no point looking for a closer node.
                if( distance > 1 )
                {
                    for( int j = closestNodeIndex + 1; j < newDataServers.Count; ++j )
                    {
                        if( newDataServers[i].DistanceFrom(newDataServers[j]) < 2 )
                        {
                            closestNodeIndex = j;
                            break;
                        }
                    }
                    // Swap the closest one with the next one.
                    DataServerInfo temp = newDataServers[i + 1];
                    newDataServers[i + 1] = newDataServers[closestNodeIndex];
                    newDataServers[closestNodeIndex] = temp;
                }
            }

            if( _log.IsInfoEnabled )
            {
                foreach( DataServerInfo server in newDataServers )
                    _log.InfoFormat("Assigned data server for block {0}: {1}", blockId, server.Address);
            }

            return new BlockAssignment(blockId, (from server in newDataServers select server.Address));
        }

        private DataServerInfo SelectClosestServerWithMinimumDistance(IEnumerable<DataServerInfo> eligibleServers, string writerHostName, string writerRackId, int minimumDistance)
        {
            lock( _random )
            {
                DataServerInfo result = (from server in eligibleServers
                                         let serverDistance = server.DistanceFrom(writerHostName, writerRackId)
                                         where serverDistance >= minimumDistance
                                         select new { Server=server, Distance=serverDistance }).OrderBy(s => s.Distance).ThenBy(s => s.Server.PendingBlocks.Count).ThenBy(s => _random.Next()).First().Server;
                return result;
            }
        }

        private DataServerInfo SelectRandomServer(IEnumerable<DataServerInfo> eligibleServers)
        {
            lock( _random )
            {
                return (from server in eligibleServers
                        orderby server.PendingBlocks.Count ascending, _random.Next() ascending
                        select server).First();
            }
        }
    }
}
