// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
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
                Assert.IsTrue(nameServer.SafeMode);
                Assert.IsFalse(cluster.Client.WaitForSafeModeOff(500));
                Utilities.TraceLineAndFlush("Starting data servers");
                cluster.StartDataServers(1);
                Utilities.TraceLineAndFlush("Data servers started");
                Assert.IsTrue(cluster.Client.WaitForSafeModeOff(Timeout.Infinite));
                Utilities.TraceLineAndFlush("Safe mode off");
                Assert.IsFalse(nameServer.SafeMode);
            }
            finally
            {
                if (cluster != null)
                    cluster.Shutdown();
            }
        }
    }
}
