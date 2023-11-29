// Copyright (c) Sven Groot (Ookii.org)
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
[Category("ClusterTest")]
public class DataServerDeathTests
{
    private const int _dataServers = 4;
    private const int _replicationFactor = 3;
    private TestDfsCluster _cluster;
    private INameServerClientProtocol _nameServer;

    [OneTimeSetUp]
    public void Setup()
    {
        _cluster = new TestDfsCluster(_dataServers, _replicationFactor, 1048576); // 1 MB block size.
        Utilities.TraceLineAndFlush("Starting cluster.");
        DfsConfiguration config = TestDfsCluster.CreateClientConfig();
        _nameServer = DfsClient.CreateNameServerClient(config);
        _cluster.Client.WaitForSafeModeOff(Timeout.Infinite);
        Utilities.TraceLineAndFlush("Cluster started.");
    }

    [OneTimeTearDown]
    public void Teardown()
    {
        Utilities.TraceLineAndFlush("Shutting down cluster.");
        _cluster.Shutdown();
        Utilities.TraceLineAndFlush("Cluster shut down.");
    }

    [Test]
    public void TestDataServerDeath()
    {
        Utilities.TraceLineAndFlush("Writing file.");
        using (DfsOutputStream stream = new DfsOutputStream(_nameServer, "/testfile"))
        {
            Utilities.GenerateData(stream, 10000000);
        }
        DfsMetrics metrics = _nameServer.GetMetrics();
        Assert.That(metrics.TotalBlockCount, Is.EqualTo(10));
        Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));

        Utilities.TraceLineAndFlush("Shutting down data server.");
        ServerAddress address = _cluster.ShutdownDataServer(_dataServers - 1);
        Assert.That(_nameServer.GetDataServerBlocks(address).Length, Is.GreaterThan(0));
        _nameServer.RemoveDataServer(address);
        metrics = _nameServer.GetMetrics();
        Assert.That(metrics.UnderReplicatedBlockCount, Is.GreaterThan(0));
        Assert.That(metrics.DataServers.Count, Is.EqualTo(_dataServers - 1));
        Utilities.TraceLineAndFlush(string.Format("Waiting for re-replication of {0} blocks.", metrics.UnderReplicatedBlockCount));
        for (int x = 0; x < 10; ++x)
        {
            Thread.Sleep(5000);
            metrics = _nameServer.GetMetrics();
            if (metrics.UnderReplicatedBlockCount == 0)
            {
                break;
            }
        }
        metrics = _nameServer.GetMetrics();
        Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
        Utilities.TraceLineAndFlush("Re-replication successful.");

        Utilities.TraceLineAndFlush("Shutting down another server.");
        address = _cluster.ShutdownDataServer(_dataServers - 2);
        _nameServer.RemoveDataServer(address);
        Assert.That(_nameServer.SafeMode, Is.True); // Safe mode re-enabled when number of data servers is less than replication factor.
    }
}
