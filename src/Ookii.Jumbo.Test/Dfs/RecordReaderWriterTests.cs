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
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture(Description = "Tests reading and writing DFS data with record readers using various record stream options.")]
[Category("ClusterTest")]
public class RecordReaderWriterTests
{
    private TestDfsCluster _cluster;
    private DfsClient _dfsClient;
    private List<Utf8String> _records;
    private const int _blockSize = 16 * (int)BinarySize.Mebi;

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
        using (Stream stream = _dfsClient.CreateFile(fileName, 0, 0))
        using (TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream))
        {
            foreach (Utf8String record in _records)
            {
                writer.WriteRecord(record);
            }

            Assert.That(writer.RecordsWritten, Is.EqualTo(_records.Count));
            Assert.That(writer.OutputBytes, Is.EqualTo(_records.Count * recordSize));
            Assert.That(writer.BytesWritten, Is.EqualTo(writer.OutputBytes));
            Assert.That(stream.Length, Is.EqualTo(writer.OutputBytes));
        }

        TestLineRecordReader(fileName);
    }

    [Test]
    public void TestLineRecordReaderRecordsDoNotCrossBoundary()
    {
        const string fileName = "/linesboundary";
        int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
        using (Stream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary))
        using (TextRecordWriter<Utf8String> writer = new TextRecordWriter<Utf8String>(stream))
        {
            foreach (Utf8String record in _records)
            {
                writer.WriteRecord(record);
            }

            int blockPadding = _blockSize % recordSize;
            int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

            Assert.That(writer.RecordsWritten, Is.EqualTo(_records.Count));
            Assert.That(writer.OutputBytes, Is.EqualTo(_records.Count * recordSize));
            Assert.That(writer.BytesWritten, Is.EqualTo(writer.OutputBytes + totalPadding));
            Assert.That(stream.Length, Is.EqualTo(writer.BytesWritten));
        }

        TestLineRecordReader(fileName);
    }


    [Test]
    public void TestLineRecordReaderByteOrderMark()
    {
        const string fileName = "/linesbom";

        using (Stream stream = _dfsClient.CreateFile(fileName))
        using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(true)))
        {
            foreach (Utf8String record in _records)
            {
                writer.WriteLine(record);
            }
        }

        TestLineRecordReader(fileName);
    }

    [Test]
    public void TestBinaryRecordReaderRecordsDoNotCrossBoundary()
    {
        const string fileName = "/binaryboundary";
        int recordSize = _records[0].ByteLength + 2; // BinaryRecordWriter writes string length which will take 2 bytes.
        using (Stream stream = _dfsClient.CreateFile(fileName, 0, 0, RecordStreamOptions.DoNotCrossBoundary))
        using (BinaryRecordWriter<Utf8String> writer = new BinaryRecordWriter<Utf8String>(stream))
        {
            foreach (Utf8String record in _records)
            {
                writer.WriteRecord(record);
            }

            int blockPadding = _blockSize % recordSize;
            int totalPadding = (int)(stream.Length / _blockSize) * blockPadding;

            Assert.That(writer.RecordsWritten, Is.EqualTo(_records.Count));
            Assert.That(writer.OutputBytes, Is.EqualTo(_records.Count * recordSize));
            Assert.That(writer.BytesWritten, Is.EqualTo(writer.OutputBytes + totalPadding));
            Assert.That(stream.Length, Is.EqualTo(writer.BytesWritten));
        }

        int recordIndex = 0;
        JumboFile file = _dfsClient.NameServer.GetFileInfo(fileName);
        int blocks = file.Blocks.Length;
        int totalRecordsRead = 0;
        for (int block = 0; block < blocks; ++block)
        {
            int offset = block * _blockSize;
            int size = Math.Min((int)(file.Size - offset), _blockSize);
            using (Stream stream = _dfsClient.OpenFile(fileName))
            using (BinaryRecordReader<Utf8String> reader = new BinaryRecordReader<Utf8String>(stream, block * _blockSize, size, true))
            {
                foreach (Utf8String record in reader.EnumerateRecords())
                {
                    Assert.That(record, Is.EqualTo(_records[recordIndex]));
                    ++recordIndex;
                }

                totalRecordsRead += reader.RecordsRead;
                int recordCount = size / recordSize;
                Assert.That(reader.RecordsRead, Is.EqualTo(recordCount));
                Assert.That(reader.InputBytes, Is.EqualTo(recordCount * recordSize));
                Assert.That(reader.BytesRead, Is.GreaterThanOrEqualTo(recordCount * recordSize));
                Assert.That(reader.BytesRead, Is.EqualTo(size));
                Assert.That(((DfsInputStream)stream).BlocksRead, Is.EqualTo(1));
            }
        }

        Assert.That(totalRecordsRead, Is.EqualTo(_records.Count));
    }

    private void TestLineRecordReader(string fileName, bool bom = false)
    {
        int recordSize = _records[0].ByteLength + Environment.NewLine.Length;
        int recordIndex = 0;
        JumboFile file = _dfsClient.NameServer.GetFileInfo(fileName);
        int blocks = file.Blocks.Length;
        int totalRecordsRead = 0;
        for (int block = 0; block < blocks; ++block)
        {
            int offset = block * _blockSize;
            int size = Math.Min((int)(file.Size - offset), _blockSize);
            using (Stream stream = _dfsClient.OpenFile(fileName))
            using (LineRecordReader reader = new LineRecordReader(stream, block * _blockSize, size, true))
            {
                foreach (Utf8String record in reader.EnumerateRecords())
                {
                    Assert.That(record, Is.EqualTo(_records[recordIndex]));
                    ++recordIndex;
                }

                totalRecordsRead += reader.RecordsRead;
                int recordCount;
                if (file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary)
                {
                    recordCount = size / recordSize;
                }
                else
                {
                    int firstRecord = offset == 0 ? 0 : (offset / recordSize) + 1;
                    int lastRecord = ((offset + size) / recordSize);
                    if (offset + size < file.Size)
                    {
                        ++lastRecord;
                    }

                    recordCount = lastRecord - firstRecord;
                }
                Assert.That(reader.RecordsRead, Is.EqualTo(recordCount));
                Assert.That(reader.InputBytes, Is.EqualTo(recordCount * recordSize));
                Assert.That(reader.BytesRead, Is.GreaterThanOrEqualTo(recordCount * recordSize + (file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary ? 0 : (recordSize - offset % recordSize))));
                Assert.That(reader.BytesRead, Is.EqualTo(stream.Position - offset));
                Assert.That(((DfsInputStream)stream).BlocksRead, Is.EqualTo((file.RecordOptions == RecordStreamOptions.DoNotCrossBoundary || block == blocks - 1) ? 1 : 2));
            }
        }
        Assert.That(totalRecordsRead, Is.EqualTo(_records.Count));
    }
}
