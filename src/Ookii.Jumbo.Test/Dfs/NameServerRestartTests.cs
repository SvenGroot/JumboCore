// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

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
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat"))
            using (MemoryStream input = new MemoryStream())
            {
                Utilities.GenerateData(input, size);
                input.Position = 0;
                Utilities.CopyStream(input, output);
            }

            const int customBlockSize = 16 * 1024 * 1024;
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo2.dat", customBlockSize, 0))
            using (MemoryStream input = new MemoryStream())
            {
                Utilities.GenerateData(input, size);
                input.Position = 0;
                Utilities.CopyStream(input, output);
            }

            JumboFile file;
            DfsMetrics metrics;
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/pending.dat"))
            {
                nameServer = null;
                Thread.Sleep(1000);
                cluster.Shutdown();
                cluster = null;
                Thread.Sleep(1000);
                cluster = new TestDfsCluster(1, 1, null, false);
                nameServer = DfsClient.CreateNameServerClient(TestDfsCluster.CreateClientConfig());
                cluster.Client.WaitForSafeModeOff(Timeout.Infinite);

                Assert.That(nameServer.GetDirectoryInfo("/").DateCreated, Is.EqualTo(rootCreatedDate));

                file = nameServer.GetFileInfo("/test2/pending.dat");
                Assert.That(file.IsOpenForWriting, Is.True);

                metrics = nameServer.GetMetrics();
                Assert.That(metrics.PendingBlockCount, Is.EqualTo(1));

                // The reason this works even though the data server is also restarted is because we didn't start writing before,
                // so the stream hadn't connected to the data server yet.
                Utilities.GenerateData(output, size);
            }
            file = nameServer.GetFileInfo("/test2/pending.dat");
            //Assert.IsFalse(file.IsOpenForWriting);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.BlockSize, Is.EqualTo(nameServer.BlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));
            Assert.That(file.Blocks.Length, Is.EqualTo(1));
            Assert.That(nameServer.GetDirectoryInfo("/test1"), Is.Null);
            Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = nameServer.GetDirectoryInfo("/test2");
            Assert.That(dir, Is.Not.Null);
            Assert.That(dir.Children.Length, Is.EqualTo(3));
            file = nameServer.GetFileInfo("/test2/foo.dat");
            Assert.That(file, Is.Not.Null);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.Blocks.Length, Is.EqualTo(1));
            Assert.That(file.BlockSize, Is.EqualTo(nameServer.BlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));
            file = nameServer.GetFileInfo("/test2/foo2.dat");
            Assert.That(file, Is.Not.Null);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.Blocks.Length, Is.EqualTo(2));
            Assert.That(file.BlockSize, Is.EqualTo(customBlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));

            Assert.That(nameServer.GetDirectoryInfo("/test2/test1"), Is.Null);
            Assert.That(nameServer.GetDirectoryInfo("/test3"), Is.Not.Null);
            metrics = nameServer.GetMetrics();
            Assert.That(metrics.TotalSize, Is.EqualTo(size * 3));
            Assert.That(metrics.TotalBlockCount, Is.EqualTo(4));
            Assert.That(metrics.PendingBlockCount, Is.EqualTo(0));
            Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
            Assert.That(metrics.DataServers.Count, Is.EqualTo(1));
        }
        finally
        {
            if (cluster != null)
            {
                cluster.Shutdown();
            }
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
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo.dat"))
            using (MemoryStream input = new MemoryStream())
            {
                Utilities.GenerateData(input, size);
                input.Position = 0;
                Utilities.CopyStream(input, output);
            }

            const int customBlockSize = 16 * 1024 * 1024;
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/foo2.dat", customBlockSize, 1))
            using (MemoryStream input = new MemoryStream())
            {
                Utilities.GenerateData(input, size);
                input.Position = 0;
                Utilities.CopyStream(input, output);
            }


            JumboFile file;
            DfsMetrics metrics;
            using (DfsOutputStream output = new DfsOutputStream(nameServer, "/test2/pending.dat"))
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

            Assert.That(nameServer.GetDirectoryInfo("/").DateCreated, Is.EqualTo(rootCreatedDate));

            file = nameServer.GetFileInfo("/test2/pending.dat");
            Assert.That(file.IsOpenForWriting, Is.False);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.Blocks.Length, Is.EqualTo(1));
            Assert.That(nameServer.GetDirectoryInfo("/test1"), Is.Null);
            Assert.That(file.BlockSize, Is.EqualTo(nameServer.BlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));
            Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = nameServer.GetDirectoryInfo("/test2");
            Assert.That(dir, Is.Not.Null);
            Assert.That(dir.Children.Length, Is.EqualTo(3));
            file = nameServer.GetFileInfo("/test2/foo.dat");
            Assert.That(file, Is.Not.Null);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.Blocks.Length, Is.EqualTo(1));
            Assert.That(file.BlockSize, Is.EqualTo(nameServer.BlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));
            file = nameServer.GetFileInfo("/test2/foo2.dat");
            Assert.That(file, Is.Not.Null);
            Assert.That(file.Size, Is.EqualTo(size));
            Assert.That(file.Blocks.Length, Is.EqualTo(2));
            Assert.That(file.BlockSize, Is.EqualTo(customBlockSize));
            Assert.That(file.ReplicationFactor, Is.EqualTo(1));
            Assert.That(nameServer.GetDirectoryInfo("/test2/test1"), Is.Null);
            Assert.That(nameServer.GetDirectoryInfo("/test3"), Is.Not.Null);
            metrics = nameServer.GetMetrics();
            Assert.That(metrics.TotalSize, Is.EqualTo(size * 3));
            Assert.That(metrics.TotalBlockCount, Is.EqualTo(4));
            Assert.That(metrics.PendingBlockCount, Is.EqualTo(0));
            Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
            Assert.That(metrics.DataServers.Count, Is.EqualTo(1));
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
