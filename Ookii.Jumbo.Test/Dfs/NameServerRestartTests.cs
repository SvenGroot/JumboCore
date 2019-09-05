// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Threading;
using System.IO;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    [Category("ClusterTest")]
    public class NameServerRestartTests
    {
        [Test]
        public void TestClusterRestart()
        {
            TestDfsCluster cluster = null;
            try
            {
                cluster = new TestDfsCluster(1, 1);
                DfsClient client = cluster.Client;
                INameServerClientProtocol nameServer = client.NameServer;
                client.WaitForSafeModeOff(Timeout.Infinite);
                DateTime rootCreatedDate = nameServer.GetDirectoryInfo("/").DateCreated;
                nameServer.CreateDirectory("/test1");
                nameServer.CreateDirectory("/test2");
                nameServer.CreateDirectory("/test1/test2");
                nameServer.Delete("/test1", true);
                nameServer.CreateDirectory("/test2/test1");
                nameServer.Move("/test2/test1", "/test3");
                const int size = 20000000;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat") )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }

                const int customBlockSize = 16 * 1024 * 1024;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo2.dat", customBlockSize, 0) )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }

                JumboFile file;
                DfsMetrics metrics;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/pending.dat") )
                {
                    nameServer = null;
                    Thread.Sleep(1000);
                    cluster.Shutdown();
                    cluster = null;
                    Thread.Sleep(1000);
                    cluster = new TestDfsCluster(1, 1, null, false);
                    nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                    cluster.Client.WaitForSafeModeOff(Timeout.Infinite);

                    Assert.AreEqual(rootCreatedDate, nameServer.GetDirectoryInfo("/").DateCreated);

                    file = nameServer.GetFileInfo("/test2/pending.dat");
                    Assert.IsTrue(file.IsOpenForWriting);

                    metrics = nameServer.GetMetrics();
                    Assert.AreEqual(1, metrics.PendingBlockCount);

                    // The reason this works even though the data server is also restarted is because we didn't start writing before,
                    // so the stream hadn't connected to the data server yet.
                    Utilities.GenerateData(output, size);
                }
                file = nameServer.GetFileInfo("/test2/pending.dat");
                //Assert.IsFalse(file.IsOpenForWriting);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(nameServer.BlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test1"));
                Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = nameServer.GetDirectoryInfo("/test2");
                Assert.IsNotNull(dir);
                Assert.AreEqual(3, dir.Children.Count);
                file = nameServer.GetFileInfo("/test2/foo.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.AreEqual(nameServer.BlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);
                file = nameServer.GetFileInfo("/test2/foo2.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(2, file.Blocks.Count);
                Assert.AreEqual(customBlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);

                Assert.IsNull(nameServer.GetDirectoryInfo("/test2/test1"));
                Assert.IsNotNull(nameServer.GetDirectoryInfo("/test3"));
                metrics = nameServer.GetMetrics();
                Assert.AreEqual(size * 3, metrics.TotalSize);
                Assert.AreEqual(4, metrics.TotalBlockCount);
                Assert.AreEqual(0, metrics.PendingBlockCount);
                Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
                Assert.AreEqual(1, metrics.DataServers.Count);
            }
            finally
            {
                if( cluster != null )
                    cluster.Shutdown();
            }
        }

        [Test]
        public void TestClusterRestartWithCheckpoint()
        {
            TestDfsCluster cluster = null;
            try
            {
                cluster = new TestDfsCluster(1, 1);
                INameServerClientProtocol nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                cluster.Client.WaitForSafeModeOff(Timeout.Infinite);
                DateTime rootCreatedDate = nameServer.GetDirectoryInfo("/").DateCreated;
                nameServer.CreateDirectory("/test1");
                nameServer.CreateDirectory("/test2");
                nameServer.CreateDirectory("/test1/test2");
                nameServer.Delete("/test1", true);
                nameServer.CreateDirectory("/test2/test1");
                nameServer.Move("/test2/test1", "/test3");
                const int size = 20000000;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat") )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }

                const int customBlockSize = 16 * 1024 * 1024;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo2.dat", customBlockSize, 1) )
                using( MemoryStream input = new MemoryStream() )
                {
                    Utilities.GenerateData(input, size);
                    input.Position = 0;
                    Utilities.CopyStream(input, output);
                }


                JumboFile file;
                DfsMetrics metrics;
                using( DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/pending.dat") )
                {
                    nameServer.CreateCheckpoint();

                    // We write data after the checkpoint to check if it will correctly process the log file after this too.
                    Utilities.GenerateData(output, size);
                }

                nameServer = null;
                Thread.Sleep(1000);
                cluster.Shutdown();
                cluster = null;
                Thread.Sleep(1000);
                cluster = new TestDfsCluster(1, 1, null, false);
                nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                cluster.Client.WaitForSafeModeOff(Timeout.Infinite);

                Assert.AreEqual(rootCreatedDate, nameServer.GetDirectoryInfo("/").DateCreated);

                file = nameServer.GetFileInfo("/test2/pending.dat");
                Assert.IsFalse(file.IsOpenForWriting);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test1"));
                Assert.AreEqual(nameServer.BlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);
                Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = nameServer.GetDirectoryInfo("/test2");
                Assert.IsNotNull(dir);
                Assert.AreEqual(3, dir.Children.Count);
                file = nameServer.GetFileInfo("/test2/foo.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.AreEqual(nameServer.BlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);
                file = nameServer.GetFileInfo("/test2/foo2.dat");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.AreEqual(2, file.Blocks.Count);
                Assert.AreEqual(customBlockSize, file.BlockSize);
                Assert.AreEqual(1, file.ReplicationFactor);
                Assert.IsNull(nameServer.GetDirectoryInfo("/test2/test1"));
                Assert.IsNotNull(nameServer.GetDirectoryInfo("/test3"));
                metrics = nameServer.GetMetrics();
                Assert.AreEqual(size * 3, metrics.TotalSize);
                Assert.AreEqual(4, metrics.TotalBlockCount);
                Assert.AreEqual(0, metrics.PendingBlockCount);
                Assert.AreEqual(0, metrics.UnderReplicatedBlockCount);
                Assert.AreEqual(1, metrics.DataServers.Count);
            }
            finally
            {
                if( cluster != null )
                    cluster.Shutdown();
            }
        }
    }
}
