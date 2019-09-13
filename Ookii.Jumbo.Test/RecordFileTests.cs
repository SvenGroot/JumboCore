// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    class RecordFileTests
    {
        [Test]
        public void TestRecordFileReaderWriter()
        {
            const int recordCount = 1000;
            List<string> records = Utilities.GenerateTextData(100, recordCount);

            byte[] data;
            long headerSize;
            using( MemoryStream stream = new MemoryStream() )
            {
                using( RecordFileWriter<Utf8String> writer = new RecordFileWriter<Utf8String>(stream) )
                {
                    Assert.AreEqual(typeof(Utf8String), writer.Header.RecordType);
                    Assert.AreEqual(typeof(Utf8String).FullName + ", " + typeof(Utf8String).Assembly.GetName().Name, writer.Header.RecordTypeName);
                    Assert.AreEqual(1, writer.Header.Version);
                    Assert.AreEqual(0, writer.RecordsWritten);
                    Assert.AreNotEqual(0, writer.OutputBytes); // Because it must've written the header this isn't 0.
                    headerSize = writer.OutputBytes;
                    Utf8String record = new Utf8String();
                    foreach( string item in records )
                    {
                        record.Set(item);
                        writer.WriteRecord(record);
                    }

                    Assert.AreEqual(recordCount, writer.RecordsWritten);
                }
                data = stream.ToArray();
            }

            const int recordSize = 105; // 100 ASCII characters + 4 byte prefix + 1 byte string length.
            const int totalRecordSize = recordSize * recordCount;
            // Hard-coded version 1 record marker distance and size + prefix size.
            long expectedSize = totalRecordSize + (totalRecordSize / 2000 * 20) + headerSize;
            Assert.AreEqual(expectedSize, data.Length);

            List<string> result = new List<string>(recordCount);
            const int stepSize = 10000;
            int totalRecordsRead = 0;
            for( int offset = 0; offset < data.Length; offset += stepSize )
            {
                using( MemoryStream stream = new MemoryStream(data) )
                using( RecordFileReader<Utf8String> reader = new RecordFileReader<Utf8String>(stream, offset, Math.Min(stepSize, stream.Length - offset), true) )
                {
                    Assert.AreEqual(typeof(Utf8String), reader.Header.RecordType);
                    Assert.AreEqual(typeof(Utf8String).FullName + ", " + typeof(Utf8String).Assembly.GetName().Name, reader.Header.RecordTypeName);
                    Assert.AreEqual(1, reader.Header.Version);
                    foreach( Utf8String record in reader.EnumerateRecords() )
                    {
                        result.Add(record.ToString());
                    }
                    totalRecordsRead += reader.RecordsRead;
                }
            }

            Assert.AreEqual(recordCount, totalRecordsRead);
            Assert.IsTrue(Utilities.CompareList(records, result));
        }
    }
}
