// Copyright (c) Sven Groot (Ookii.org)
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
[Category("ClusterTest")]
public class NameServerSafeModeTests
{
    [Test]
    public void TestSafeMode()
    {
        TestDfsCluster cluster = null;
        try
        {
            cluster = new TestDfsCluster(0, 1);
            INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
            Utilities.TraceLineAndFlush("Cluster started");
            Assert.That(nameServer.SafeMode, Is.True);
            Assert.That(cluster.Client.WaitForSafeModeOff(500), Is.False);
            Utilities.TraceLineAndFlush("Starting data servers");
            cluster.StartDataServers(1);
            Utilities.TraceLineAndFlush("Data servers started");
            Assert.That(cluster.Client.WaitForSafeModeOff(Timeout.Infinite), Is.True);
            Utilities.TraceLineAndFlush("Safe mode off");
            Assert.That(nameServer.SafeMode, Is.False);
        }
        finally
        {
            if (cluster != null)
            {
                cluster.Shutdown();
            }
        }
    }
}
