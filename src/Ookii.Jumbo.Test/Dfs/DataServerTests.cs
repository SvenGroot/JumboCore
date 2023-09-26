// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

#pragma warning disable SYSLIB0011 // BinaryFormatter is deprecated.

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    [Category("ClusterTest")]
    public class DataServerTests
    {
        private const int _dataServers = 4;
        private const int _replicationFactor = 3;
        private TestDfsCluster _cluster;
        private INameServerClientProtocol _nameServer;

        [OneTimeSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(_dataServers, _replicationFactor);
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
        [Description("Tests uploading a file to multiple data servers, and then tries to read from both of them.")]
        public void TestFileUploadDownload()
        {
            const int size = 20000000;

            using (MemoryStream stream = new MemoryStream())
            {
                // Create a file. This size is chosen so it's not a whole number of packets.
                Utilities.TraceLineAndFlush("Creating file");
                Utilities.GenerateData(stream, size);
                stream.Position = 0;
                Utilities.TraceLineAndFlush("Uploading file");
                using (DfsOutputStream output = new DfsOutputStream(_nameServer, "/TestStreams.dat"))
                {
                    Utilities.CopyStream(stream, output);
                    Assert.AreEqual(size, output.Length);
                    Assert.AreEqual(size, output.Position);
                }

                Ookii.Jumbo.Dfs.FileSystem.JumboFile file = _nameServer.GetFileInfo("/TestStreams.dat");
                Assert.AreEqual(1, file.Blocks.Count);
                Assert.AreEqual(size, file.Size);
                ServerAddress[] servers = _nameServer.GetDataServersForBlock(file.Blocks[0]);
                Assert.AreEqual(_replicationFactor, servers.Length);
                Assert.AreNotEqual(servers[0], servers[1]);
                foreach (ServerAddress server in servers)
                    DownloadAndCompareBlock(file.Blocks[0], server, stream);
            }
        }

        private void DownloadAndCompareBlock(Guid blockID, ServerAddress server, MemoryStream dataStream)
        {
            Utilities.TraceLineAndFlush(string.Format("Comparing file for server {0}", server));
            dataStream.Position = 0;
            using (TcpClient client = new TcpClient(server.HostName, server.Port))
            using (NetworkStream stream = client.GetStream())
            using (BinaryReader reader = new BinaryReader(stream))
            {
                DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
                header.BlockId = blockID;
                header.Offset = 0;
                header.Size = (int)dataStream.Length;
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, header);
                DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt16();
                Assert.AreEqual(DataServerClientProtocolResult.Ok, result);
                int offset = reader.ReadInt32();
                Assert.AreEqual(0, offset);
                Packet packet = new Packet();
                byte[] buffer1 = new byte[Packet.PacketSize];
                byte[] buffer2 = new byte[Packet.PacketSize];
                while (!packet.IsLastPacket)
                {
                    result = (DataServerClientProtocolResult)reader.ReadInt16();
                    Assert.AreEqual(DataServerClientProtocolResult.Ok, result);
                    packet.Read(reader, PacketFormatOption.NoSequenceNumber, true);
                    packet.CopyTo(0, buffer1, 0, buffer1.Length);
                    dataStream.Read(buffer2, 0, packet.Size);
                    Assert.IsTrue(Utilities.CompareArray(buffer1, 0, buffer2, 0, packet.Size));
                }
                Assert.AreEqual(dataStream.Length, dataStream.Position);
            }
        }
    }
}
