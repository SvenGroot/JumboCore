// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using NameServerApplication;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Topology;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class ReplicaPlacementTests
{
    [Test]
    public void TestMultiRackPlacementClusterWriter()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "rack1_1";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, true);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[0].HostName, Is.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName.StartsWith("rack1"), Is.True);
        Assert.That(assignment.DataServers[2].HostName.StartsWith("rack2"), Is.True);
    }

    [Test]
    public void TestMultiRackPlacementNonClusterWriter()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "foo";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, true);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[0].HostName, Is.Not.EqualTo(writer));
        string firstNode = assignment.DataServers[0].HostName;
        string firstNodeRackId = assignment.DataServers[0].HostName.Substring(0, 5);
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(firstNode));
        Assert.That(assignment.DataServers[1].HostName.StartsWith(firstNodeRackId), Is.True);
        Assert.That(assignment.DataServers[2].HostName.StartsWith(firstNodeRackId), Is.False);
    }

    [Test]
    public void TestMultiRackPlacementNoLocalReplica()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "rack1_1";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, false);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[0].HostName.StartsWith("rack1_"), Is.True);
        string firstNode = assignment.DataServers[0].HostName;
        string firstNodeRackId = assignment.DataServers[0].HostName.Substring(0, 5);
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(writer)); // If the local node is randomly selected, it must be the first one in the list
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(firstNode));
        Assert.That(assignment.DataServers[1].HostName.StartsWith(firstNodeRackId), Is.True);
        Assert.That(assignment.DataServers[2].HostName.StartsWith(firstNodeRackId), Is.False);
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(writer)); // If the local node is randomly selected, it must be the first one in the list
    }


    [Test]
    public void TestReReplicationMultiRackPlacement()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(2, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        file.Blocks.Add(block.BlockId);

        // Both existing replicas are on the same rack, so it has to assign to a different rack.
        dataServers[new ServerAddress("rack1_3", 9000)].Blocks.Add(block.BlockId);
        dataServers[new ServerAddress("rack1_5", 9000)].Blocks.Add(block.BlockId);

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, null, true);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(1));
        Assert.That(assignment.DataServers[0].HostName.StartsWith("rack1"), Is.False);
    }

    [Test]
    public void TestSingleRackPlacementNonClusterWriter()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(1, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "foo";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, true);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[0].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(assignment.DataServers[0].HostName));
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(assignment.DataServers[1].HostName));
    }

    [Test]
    public void TestSingleRackPlacementClusterWriter()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(1, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "rack1_1";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, true);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[0].HostName, Is.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(assignment.DataServers[2].HostName));
    }

    [Test]
    public void TestSingleRackPlacementNoLocalReplica()
    {
        Dictionary<ServerAddress, DataServerInfo> dataServers;
        ReplicaPlacement target = CreateReplicaPlacement(1, 5, out dataServers);
        DfsFile file = CreateFakeFile(3);

        BlockInfo block = new BlockInfo(Guid.NewGuid(), file);

        const string writer = "rack1_1";

        BlockAssignment assignment = target.AssignBlockToDataServers(dataServers.Values, block, writer, false);
        Assert.That(assignment.BlockId, Is.EqualTo(block.BlockId));
        Assert.That(assignment.DataServers.Length, Is.EqualTo(3));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(writer)); // If it's randomly picked, it must be the first one.
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(writer));
        Assert.That(assignment.DataServers[1].HostName, Is.Not.EqualTo(assignment.DataServers[0].HostName));
        Assert.That(assignment.DataServers[2].HostName, Is.Not.EqualTo(assignment.DataServers[1].HostName));
    }

    private ReplicaPlacement CreateReplicaPlacement(int racks, int nodesPerRack, out Dictionary<ServerAddress, DataServerInfo> dataServers)
    {
        dataServers = new Dictionary<ServerAddress, DataServerInfo>();

        JumboConfiguration config = new JumboConfiguration();
        for (int rack = 0; rack < racks; ++rack)
        {
            config.PatternTopologyResolver.Racks.Add(new RackConfigurationElement() { RackId = string.Format("rack{0}", rack + 1), Pattern = string.Format(@"^rack{0}_\d$", rack + 1) });
        }

        NetworkTopology topology = new NetworkTopology(config);


        for (int rack = 0; rack < racks; ++rack)
        {
            for (int node = 0; node < nodesPerRack; ++node)
            {
                DataServerInfo server = new DataServerInfo(new ServerAddress(string.Format("rack{0}_{1}", rack + 1, node + 1), 9000), Guid.Empty);
                server.DiskSpaceTotal = 100L * 1024 * 1024 * 1024;
                server.DiskSpaceFree = 100L * 1024 * 1024 * 1024;
                server.HasReportedBlocks = true;
                dataServers.Add(server.Address, server);
                topology.AddNode(server);
            }
        }

        return new ReplicaPlacement(new DfsConfiguration(), topology);
    }

    private DfsFile CreateFakeFile(int replicationFactor)
    {
        DfsDirectory root = new DfsDirectory(null, "", DateTime.UtcNow);
        return new DfsFile(root, "testfile", DateTime.UtcNow, 16 * 1024 * 1024, replicationFactor, IO.RecordStreamOptions.None);
    }

}
