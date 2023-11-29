// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Dfs;

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
                Assert.That(output.Length, Is.EqualTo(size));
                Assert.That(output.Position, Is.EqualTo(size));
            }

            Ookii.Jumbo.Dfs.FileSystem.JumboFile file = _nameServer.GetFileInfo("/TestStreams.dat");
            Assert.That(file.Blocks.Length, Is.EqualTo(1));
            Assert.That(file.Size, Is.EqualTo(size));
            ServerAddress[] servers = _nameServer.GetDataServersForBlock(file.Blocks[0]);
            Assert.That(servers.Length, Is.EqualTo(_replicationFactor));
            Assert.That(servers[1], Is.Not.EqualTo(servers[0]));
            foreach (ServerAddress server in servers)
            {
                DownloadAndCompareBlock(file.Blocks[0], server, stream);
            }
        }
    }

    private void DownloadAndCompareBlock(Guid blockID, ServerAddress server, MemoryStream dataStream)
    {
        Utilities.TraceLineAndFlush(string.Format("Comparing file for server {0}", server));
        dataStream.Position = 0;
        using TcpClient client = new TcpClient(server.HostName, server.Port);
        using NetworkStream stream = client.GetStream();
        using BinaryReader reader = new BinaryReader(stream);
        using var writer = new BinaryWriter(stream);
        DataServerClientProtocolReadHeader header = new DataServerClientProtocolReadHeader();
        header.BlockId = blockID;
        header.Offset = 0;
        header.Size = (int)dataStream.Length;
        ValueWriter.WriteValue<DataServerClientProtocolHeader>(header, writer);
        DataServerClientProtocolResult result = (DataServerClientProtocolResult)reader.ReadInt16();
        Assert.That(result, Is.EqualTo(DataServerClientProtocolResult.Ok));
        int offset = reader.ReadInt32();
        Assert.That(offset, Is.EqualTo(0));
        Packet packet = new Packet();
        byte[] buffer1 = new byte[Packet.PacketSize];
        byte[] buffer2 = new byte[Packet.PacketSize];
        while (!packet.IsLastPacket)
        {
            result = (DataServerClientProtocolResult)reader.ReadInt16();
            Assert.That(result, Is.EqualTo(DataServerClientProtocolResult.Ok));
            packet.Read(reader, PacketFormatOption.NoSequenceNumber, true);
            packet.CopyTo(0, buffer1, 0, buffer1.Length);
            dataStream.Read(buffer2, 0, packet.Size);
            Assert.That(Utilities.CompareArray(buffer1, 0, buffer2, 0, packet.Size), Is.True);
        }
        Assert.That(dataStream.Position, Is.EqualTo(dataStream.Length));
    }
}
