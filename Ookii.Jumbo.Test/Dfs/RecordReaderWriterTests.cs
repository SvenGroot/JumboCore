// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using System.Diagnostics;
using System.Threading;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs.FileSystem;
using System.IO;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture(Description="Tests reading and writing DFS data with record readers using various record stream options.")]
    [Category("ClusterTest")]
    public class RecordReaderWriterTests
    {
        private TestDfsCluster _cluster;
        private DfsClient _dfsClient;
        private List<Utf8String> _records;
        private const int _blockSize = 16 * (int)BinarySize.Megabyte;

        [OneTimeSetUp]
        public void Setup()
        {
            Trace.AutoFlush = true;
            _cluster = new TestDfsCluster(1, 1, _blockSize);
            Trace.WriteLine("Starting nameserver.");
            _dfsClient = _cluster.Client;
            _dfsClient.WaitForSafeModeOff(Timeout.Infinite);
            Trace.WriteLine("Name server running.");
            _records = Utilities.GenerateUtf8TextData(100000, 1000).ToList();
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
        public void TestLineRecordReader()
        {
            const string fileName = "/lines";
            int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
            using( Stream stream = _dfsClient.CreateFile(fileName, 0, 0) )
            using( TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes, writer.BytesWritten);
                Assert.AreEqual(writer.OutputBytes, stream.Length);
            }

            TestLineRecordReader(fileName);
        }

        [Test]
        public void TestLineRecordReaderRecordsDoNotCrossBoundary()
        {
            const string fileName = "/linesboundary";
            int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
            using( Stream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary) )
            using( TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                int blockPadding = _blockSize % recordSize;
                int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes + totalPadding, writer.BytesWritten);
                Assert.AreEqual(writer.BytesWritten, stream.Length);
            }

            TestLineRecordReader(fileName);
        }


        [Test]
        public void TestLineRecordReaderByteOrderMark()
        {
            const string fileName = "/linesbom";

            using( Stream stream = _dfsClient.CreateFile(fileName) )
            using( StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(true)) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteLine(record);
            }

            TestLineRecordReader(fileName);
        }

        [Test]
        public void TestBinaryRecordReaderRecordsDoNotCrossBoundary()
        {
            const string fileName = "/binaryboundary";
            int recordSize = _records[0].ByteLength + 2; // BinaryRecordWriter writes string length which will take 2 bytes.
            using( Stream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary) )
            using( BinaryRecordWriter<Utf8String> writer = new BinaryRecordWriter<Utf8String>(stream) )
            {
                foreach( Utf8String record in _records )
                    writer.WriteRecord(record);

                int blockPadding = _blockSize % recordSize;
                int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

                Assert.AreEqual(_records.Count, writer.RecordsWritten);
                Assert.AreEqual(_records.Count * recordSize, writer.OutputBytes);
                Assert.AreEqual(writer.OutputBytes + totalPadding, writer.BytesWritten);
                Assert.AreEqual(writer.BytesWritten, stream.Length);
            }

            int recordIndex = 0;
            JumboFile file = _dfsClient.NameServer.GetFileInfo(fileName);
            int blocks = file.Blocks.Count;
            int totalRecordsRead = 0;
            for( int block = 0; block < blocks; ++block )
            {
                int offset = block * _blockSize;
                int size = Math.Min((int)(file.Size - offset), _blockSize);
                using( Stream stream = _dfsClient.OpenFile(fileName) )
                using( BinaryRecordReader<Utf8String> reader = new BinaryRecordReader<Utf8String>(stream, block * _blockSize, size, true) )
                {
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        Assert.AreEqual(_records[recordIndex], record);
                        ++recordIndex;
                    }

                    totalRecordsRead += reader.RecordsRead;
                    int recordCount = size / recordSize;
                    Assert.AreEqual(recordCount, reader.RecordsRead);
                    Assert.AreEqual(recordCount * recordSize, reader.InputBytes);
                    Assert.GreaterOrEqual(reader.BytesRead, recordCount * recordSize);
                    Assert.AreEqual(size, reader.BytesRead);
                    Assert.AreEqual(1, ((DfsInputStream)stream).BlocksRead);
                }
            }

            Assert.AreEqual(_records.Count, totalRecordsRead);
        }

        private void TestLineRecordReader(string fileName, bool bom = false)
        {
            int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
            int recordIndex = 0;
            JumboFile file = _dfsClient.NameServer.GetFileInfo(fileName);
            int blocks = file.Blocks.Count;
            int totalRecordsRead = 0;
            for( int block = 0; block < blocks; ++block )
            {
                int offset = block * _blockSize;
                int size = Math.Min((int)(file.Size - offset), _blockSize);
                using( Stream stream = _dfsClient.OpenFile(fileName) )
                using( LineRecordReader reader = new LineRecordReader(stream, block * _blockSize, size, true) )
                {
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        Assert.AreEqual(_records[recordIndex], record);
                        ++recordIndex;
                    }

                    totalRecordsRead += reader.RecordsRead;
                    int recordCount;
                    if( file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary )
                    {
                        recordCount = size / recordSize;
                    }
                    else
                    {
                        int firstRecord = offset == 0 ? 0 : (offset / recordSize) + 1;
                        int lastRecord = ((offset + size) / recordSize);
                        if( offset + size < file.Size )
                            ++lastRecord;
                        recordCount = lastRecord - firstRecord;
                    }
                    Assert.AreEqual(recordCount, reader.RecordsRead);
                    Assert.AreEqual(recordCount * recordSize, reader.InputBytes);
                    Assert.GreaterOrEqual(reader.BytesRead, recordCount * recordSize + (file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary ? 0 : (recordSize - offset % recordSize)));
                    Assert.AreEqual(stream.Position - offset, reader.BytesRead);
                    Assert.AreEqual((file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary || block == blocks - 1) ? 1 : 2, ((DfsInputStream)stream).BlocksRead);
                }
            }
            Assert.AreEqual(_records.Count, totalRecordsRead);
        }
    }
}
