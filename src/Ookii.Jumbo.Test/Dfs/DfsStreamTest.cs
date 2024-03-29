﻿// Copyright (c) Sven Groot (Ookii.org)
using System.Diagnostics;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
[Category("ClusterTest")]
public class DfsStreamTest
{
    private TestDfsCluster _cluster;
    private INameServerClientProtocol _nameServer;

    [OneTimeSetUp]
    public void Setup()
    {
        _cluster = new TestDfsCluster(2, 2);
        Trace.WriteLine("Starting nameserver.");
        DfsConfiguration config = TestDfsCluster.CreateClientConfig();
        _nameServer = DfsClient.CreateNameServerClient(config);
        _cluster.Client.WaitForSafeModeOff(Timeout.Infinite);
        Trace.WriteLine("Name server running.");
        Trace.Flush();
    }

    [OneTimeTearDown]
    public void Teardown()
    {
        Trace.WriteLine("Shutting down cluster.");
        Trace.Flush();
        _cluster.Shutdown();
        Trace.WriteLine("Cluster shut down.");
        Trace.Flush();
    }

    [Test]
    public void DfsOutputStreamConstructorTest()
    {
        using (DfsOutputStream stream = new DfsOutputStream(_nameServer, "/OutputStreamConstructorTest"))
        {
            Assert.That(stream.BlockSize, Is.EqualTo(_nameServer.BlockSize));
            Assert.That(stream.CanRead, Is.False);
            Assert.That(stream.CanSeek, Is.False);
            Assert.That(stream.CanWrite, Is.True);
            Assert.That(stream.Length, Is.EqualTo(0));
            Assert.That(stream.Position, Is.EqualTo(0));
        }
    }

    [Test]
    public void TestStreamsSameBufferSize()
    {
        TestStreams("/TestStreamSameBufferSize", Packet.PacketSize, 0, 0);
    }

    [Test]
    public void TestStreamsDivisibleBufferSize()
    {
        TestStreams("/TestStreamDivisibleBufferSize", Packet.PacketSize / 16, 0, 0);
    }

    [Test]
    public void TestStreamsIndivisibleBufferSize()
    {
        // Use a buffer size that's different to test Write calls that straddle the boundary.
        TestStreams("/TestStreamIndivisibleBufferSize", Packet.PacketSize / 16 + 100, 0, 0);
    }

    [Test]
    public void TestStreamsCustomBlockSize()
    {
        TestStreams("/TestStreamCustomBlockSize", Packet.PacketSize, 16 * 1024 * 1024, 0);
    }

    private void TestStreams(string fileName, int bufferSize, int blockSize, int replicationFactor)
    {
        const int size = 100000000;

        // This test exercises both DfsOutputStream and DfsInputStream by writing a file to the DFS and reading it back
        //string file = "TestStreams.dat";
        //string path = Utilities.GenerateFile(file, size);
        using (MemoryStream stream = new MemoryStream())
        {
            // Create a file. This size is chosen so it's not a whole number of packets.
            Trace.WriteLine("Creating file");
            Trace.Flush();
            Utilities.GenerateData(stream, size);
            stream.Position = 0;
            Trace.WriteLine("Uploading file");
            Trace.Flush();
            using (DfsOutputStream output = new DfsOutputStream(_nameServer, fileName, blockSize, replicationFactor))
            {
                Utilities.CopyStream(stream, output, bufferSize);
                Assert.That(output.Length, Is.EqualTo(size));
                Assert.That(output.Position, Is.EqualTo(size));
            }

            Trace.WriteLine("Comparing file");
            Trace.Flush();
            stream.Position = 0;
            using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
            {
                Assert.That(input.BlockSize, Is.EqualTo(blockSize == 0 ? _nameServer.BlockSize : blockSize));
                Assert.That(input.CanRead, Is.True);
                Assert.That(input.CanSeek, Is.True);
                Assert.That(input.CanWrite, Is.False);
                Assert.That(input.Length, Is.EqualTo(size));
                Assert.That(input.Position, Is.EqualTo(0));
                Assert.That(Utilities.CompareStream(stream, input, bufferSize), Is.True);
                Assert.That(input.Position, Is.EqualTo(size));
                Trace.WriteLine("Testing stream seek.");
                Trace.Flush();
                input.Position = 100000;
                stream.Position = 100000;
                byte[] buffer = new byte[100000];
                byte[] buffer2 = new byte[100000];
                input.Read(buffer, 0, buffer.Length);
                stream.Read(buffer2, 0, buffer.Length);
                Assert.That(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length), Is.True);
                Assert.That(input.PaddingBytesSkipped, Is.EqualTo(0));
            }
        }
    }

    [Test]
    public void DfsInputStreamErrorRecovery()
    {
        const int size = 100000000;

        using (MemoryStream stream = new MemoryStream())
        {
            // Create a file. This size is chosen so it's not a whole number of packets.
            Trace.WriteLine("Creating file");
            Trace.Flush();
            Utilities.GenerateData(stream, size);
            stream.Position = 0;
            Trace.WriteLine("Uploading file");
            Trace.Flush();
            using (DfsOutputStream output = new DfsOutputStream(_nameServer, "/DfsInputStreamErrorRecovery.dat"))
            {
                Utilities.CopyStream(stream, output);
                Assert.That(output.Length, Is.EqualTo(size));
                Assert.That(output.Position, Is.EqualTo(size));
            }

            // Make a modification so it'll cause an InvalidChecksumException
            Ookii.Jumbo.Dfs.FileSystem.JumboFile file = _nameServer.GetFileInfo("/DfsInputStreamErrorRecovery.dat");
            ServerAddress[] servers = _nameServer.GetDataServersForBlock(file.Blocks[0]);
            string blockFile = Path.Combine(Path.Combine(Utilities.TestOutputPath, "blocks" + (servers[0].Port - TestDfsCluster.FirstDataServerPort).ToString()), file.Blocks[0].ToString());
            using (FileStream fileStream = new FileStream(blockFile, FileMode.Open, FileAccess.ReadWrite))
            {
                fileStream.Position = 500000;
                int b = fileStream.ReadByte();
                fileStream.Position = 500000;
                fileStream.WriteByte((byte)(b + 10));
            }

            Trace.WriteLine("Comparing file");
            Trace.Flush();
            stream.Position = 0;
            using (DfsInputStream input = new DfsInputStream(_nameServer, "/DfsInputStreamErrorRecovery.dat"))
            {
                Assert.That(input.BlockSize, Is.EqualTo(_nameServer.BlockSize));
                Assert.That(input.CanRead, Is.True);
                Assert.That(input.CanSeek, Is.True);
                Assert.That(input.CanWrite, Is.False);
                Assert.That(input.Length, Is.EqualTo(size));
                Assert.That(input.Position, Is.EqualTo(0));
                Assert.That(Utilities.CompareStream(stream, input), Is.True);
                Assert.That(input.Position, Is.EqualTo(size));
                Assert.That(input.DataServerErrors, Is.EqualTo(1)); // We should've had one recovered error.
            }
        }
    }

    [Test]
    public void TestStreamsRecordBoundary()
    {
        const int size = 100000000;
        const string fileName = "/RecordBoundary";
        const int recordSize = 1000;
        const int blockSize = 16 * (int)BinarySize.Mebi;

        // This test exercises both DfsOutputStream and DfsInputStream by writing a file to the DFS and reading it back
        using (MemoryStream stream = new MemoryStream())
        {
            // Create a file. This size is chosen so it's not a whole number of packets.
            Trace.WriteLine("Creating file");
            Trace.Flush();
            Utilities.GenerateData(stream, size);
            stream.Position = 0;
            Trace.WriteLine("Uploading file");
            Trace.Flush();
            int realSize;
            int blockPadding = blockSize % recordSize;
            int totalPadding;
            using (DfsOutputStream output = new DfsOutputStream(_nameServer, fileName, blockSize, 0, true, IO.RecordStreamOptions.DoNotCrossBoundary))
            {
                byte[] buffer = new byte[recordSize];
                int bytesRead = 0;
                while ((bytesRead = stream.Read(buffer, 0, recordSize)) > 0)
                {
                    output.Write(buffer, 0, bytesRead);
                    output.MarkRecord();
                }

                int blocks = size / blockSize;
                if (size % blockSize != 0)
                {
                    ++blocks;
                }

                totalPadding = (blocks - 1) * blockPadding;
                realSize = size + totalPadding;
                Assert.That(output.Length, Is.EqualTo(realSize));
                Assert.That(output.Position, Is.EqualTo(realSize));
            }

            Trace.WriteLine("Comparing file");
            Trace.Flush();
            stream.Position = 0;
            using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
            {
                Assert.That(input.IsStopped, Is.False);
                Assert.That(input.BlockSize, Is.EqualTo(blockSize));
                Assert.That(input.CanRead, Is.True);
                Assert.That(input.CanSeek, Is.True);
                Assert.That(input.CanWrite, Is.False);
                Assert.That(input.Length, Is.EqualTo(realSize));
                Assert.That(input.Position, Is.EqualTo(0));
                Assert.That(Utilities.CompareStream(stream, input), Is.True);
                Assert.That(input.Position, Is.EqualTo(realSize));
                Assert.That(input.PaddingBytesSkipped, Is.EqualTo(totalPadding));
                Assert.That(input.IsStopped, Is.True);
                Trace.WriteLine("Testing stream seek.");
                Trace.Flush();
                int startPosition = blockSize - 10000;
                input.Position = startPosition;
                Assert.That(input.IsStopped, Is.False);
                stream.Position = startPosition;
                byte[] buffer = new byte[100000];
                byte[] buffer2 = new byte[100000];
                input.Read(buffer, 0, buffer.Length);
                Assert.That(input.IsStopped, Is.False);
                Assert.That(input.PaddingBytesSkipped, Is.EqualTo(totalPadding + blockPadding)); // PaddingBytesSkipped is not reset to zero.
                stream.Read(buffer2, 0, buffer.Length);
                Assert.That(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length), Is.True);
                Assert.That(input.Position, Is.EqualTo(startPosition + buffer.Length + blockPadding));
                Utilities.TraceLineAndFlush("Testing stream seek into padding.");
                startPosition = blockSize - blockPadding / 2;
                input.Position = startPosition;
                // We read from the reference stream after the last record in the first block rather than the computed position.
                stream.Position = blockSize - blockPadding;
                input.Read(buffer, 0, buffer.Length);
                stream.Read(buffer2, 0, buffer.Length);
                Assert.That(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length), Is.True);
                // Position of input should have been updated to blockSize after the seek, so the current position should reflect that.
                Assert.That(input.Position, Is.EqualTo(blockSize + buffer.Length));
            }

            stream.Position = 0;
            using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
            {
                input.StopReadingAtPosition = blockSize;
                Assert.That(input.IsStopped, Is.False);
                byte[] buffer = new byte[100000];
                byte[] buffer2 = new byte[100000];
                int bytesRead;
                int totalBytesRead = 0;
                while ((bytesRead = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    stream.Read(buffer2, 0, bytesRead);
                    Assert.That(Utilities.CompareArray(buffer, 0, buffer2, 0, bytesRead), Is.True);
                    totalBytesRead += bytesRead;
                }

                Assert.That(totalBytesRead, Is.EqualTo(blockSize - blockPadding));
                Assert.That(input.Position, Is.EqualTo(blockSize));
                Assert.That(input.BlocksRead, Is.EqualTo(1));
                Assert.That(input.Read(buffer, 0, buffer.Length), Is.EqualTo(0));
                Assert.That(input.IsStopped, Is.True);

                input.StopReadingAtPosition = input.Length;
                Assert.That(input.IsStopped, Is.False);
                bytesRead = input.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(buffer.Length));
                stream.Read(buffer2, 0, buffer2.Length);
                Assert.That(Utilities.CompareArray(buffer, 0, buffer2, 0, bytesRead), Is.True);
                Assert.That(input.IsStopped, Is.False);
            }
        }
    }
}
