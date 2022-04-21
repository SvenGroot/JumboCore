// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
                Assert.AreEqual(_nameServer.BlockSize, stream.BlockSize);
                Assert.IsFalse(stream.CanRead);
                Assert.IsFalse(stream.CanSeek);
                Assert.IsTrue(stream.CanWrite);
                Assert.AreEqual(0, stream.Length);
                Assert.AreEqual(0, stream.Position);
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
                    Assert.AreEqual(size, output.Length);
                    Assert.AreEqual(size, output.Position);
                }

                Trace.WriteLine("Comparing file");
                Trace.Flush();
                stream.Position = 0;
                using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
                {
                    Assert.AreEqual(blockSize == 0 ? _nameServer.BlockSize : blockSize, input.BlockSize);
                    Assert.IsTrue(input.CanRead);
                    Assert.IsTrue(input.CanSeek);
                    Assert.IsFalse(input.CanWrite);
                    Assert.AreEqual(size, input.Length);
                    Assert.AreEqual(0, input.Position);
                    Assert.IsTrue(Utilities.CompareStream(stream, input, bufferSize));
                    Assert.AreEqual(size, input.Position);
                    Trace.WriteLine("Testing stream seek.");
                    Trace.Flush();
                    input.Position = 100000;
                    stream.Position = 100000;
                    byte[] buffer = new byte[100000];
                    byte[] buffer2 = new byte[100000];
                    input.Read(buffer, 0, buffer.Length);
                    stream.Read(buffer2, 0, buffer.Length);
                    Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length));
                    Assert.AreEqual(0, input.PaddingBytesSkipped);
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
                    Assert.AreEqual(size, output.Length);
                    Assert.AreEqual(size, output.Position);
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
                    Assert.AreEqual(_nameServer.BlockSize, input.BlockSize);
                    Assert.IsTrue(input.CanRead);
                    Assert.IsTrue(input.CanSeek);
                    Assert.IsFalse(input.CanWrite);
                    Assert.AreEqual(size, input.Length);
                    Assert.AreEqual(0, input.Position);
                    Assert.IsTrue(Utilities.CompareStream(stream, input));
                    Assert.AreEqual(size, input.Position);
                    Assert.AreEqual(1, input.DataServerErrors); // We should've had one recovered error.
                }
            }
        }

        [Test]
        public void TestStreamsRecordBoundary()
        {
            const int size = 100000000;
            const string fileName = "/RecordBoundary";
            const int recordSize = 1000;
            const int blockSize = 16 * (int)BinarySize.Megabyte;

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
                        ++blocks;

                    totalPadding = (blocks - 1) * blockPadding;
                    realSize = size + totalPadding;
                    Assert.AreEqual(realSize, output.Length);
                    Assert.AreEqual(realSize, output.Position);
                }

                Trace.WriteLine("Comparing file");
                Trace.Flush();
                stream.Position = 0;
                using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
                {
                    Assert.IsFalse(input.IsStopped);
                    Assert.AreEqual(blockSize, input.BlockSize);
                    Assert.IsTrue(input.CanRead);
                    Assert.IsTrue(input.CanSeek);
                    Assert.IsFalse(input.CanWrite);
                    Assert.AreEqual(realSize, input.Length);
                    Assert.AreEqual(0, input.Position);
                    Assert.IsTrue(Utilities.CompareStream(stream, input));
                    Assert.AreEqual(realSize, input.Position);
                    Assert.AreEqual(totalPadding, input.PaddingBytesSkipped);
                    Assert.IsTrue(input.IsStopped);
                    Trace.WriteLine("Testing stream seek.");
                    Trace.Flush();
                    int startPosition = blockSize - 10000;
                    input.Position = startPosition;
                    Assert.IsFalse(input.IsStopped);
                    stream.Position = startPosition;
                    byte[] buffer = new byte[100000];
                    byte[] buffer2 = new byte[100000];
                    input.Read(buffer, 0, buffer.Length);
                    Assert.IsFalse(input.IsStopped);
                    Assert.AreEqual(totalPadding + blockPadding, input.PaddingBytesSkipped); // PaddingBytesSkipped is not reset to zero.
                    stream.Read(buffer2, 0, buffer.Length);
                    Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length));
                    Assert.AreEqual(startPosition + buffer.Length + blockPadding, input.Position);
                    Utilities.TraceLineAndFlush("Testing stream seek into padding.");
                    startPosition = blockSize - blockPadding / 2;
                    input.Position = startPosition;
                    // We read from the reference stream after the last record in the first block rather than the computed position.
                    stream.Position = blockSize - blockPadding;
                    input.Read(buffer, 0, buffer.Length);
                    stream.Read(buffer2, 0, buffer.Length);
                    Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, buffer.Length));
                    // Position of input should have been updated to blockSize after the seek, so the current position should reflect that.
                    Assert.AreEqual(blockSize + buffer.Length, input.Position);
                }

                stream.Position = 0;
                using (DfsInputStream input = new DfsInputStream(_nameServer, fileName))
                {
                    input.StopReadingAtPosition = blockSize;
                    Assert.IsFalse(input.IsStopped);
                    byte[] buffer = new byte[100000];
                    byte[] buffer2 = new byte[100000];
                    int bytesRead;
                    int totalBytesRead = 0;
                    while ((bytesRead = input.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        stream.Read(buffer2, 0, bytesRead);
                        Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, bytesRead));
                        totalBytesRead += bytesRead;
                    }

                    Assert.AreEqual(blockSize - blockPadding, totalBytesRead);
                    Assert.AreEqual(blockSize, input.Position);
                    Assert.AreEqual(1, input.BlocksRead);
                    Assert.AreEqual(0, input.Read(buffer, 0, buffer.Length));
                    Assert.IsTrue(input.IsStopped);

                    input.StopReadingAtPosition = input.Length;
                    Assert.IsFalse(input.IsStopped);
                    bytesRead = input.Read(buffer, 0, buffer.Length);
                    Assert.AreEqual(buffer.Length, bytesRead);
                    stream.Read(buffer2, 0, buffer2.Length);
                    Assert.IsTrue(Utilities.CompareArray(buffer, 0, buffer2, 0, bytesRead));
                    Assert.IsFalse(input.IsStopped);
                }
            }
        }
    }
}
