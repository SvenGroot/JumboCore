using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class SortSpillRecordWriterTests
    {
        #region Nested types

        private class DuplicateEliminationCombiner : ITask<int, int>
        {
            public void Run(RecordReader<int> input, RecordWriter<int> output)
            {
                int? prev = null;
                foreach( int record in input.EnumerateRecords() )
                {
                    // Eliminates duplicates. This was chosen because its correct operation depends on the
                    // input being sorted.
                    if( prev == null || prev.Value != record )
                        output.WriteRecord(record);
                    prev = record;
                }
            }
        }

        private class ReverseComparer : IRawComparer<int>
        {
            public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
            {
                return -RawComparer<int>.Comparer.Compare(buffer1, offset1, count1, buffer2, offset2, count2);
            }

            public int Compare(int x, int y)
            {
                return -RawComparer<int>.Comparer.Compare(x, y);
            }
        }


        #endregion

        [OneTimeSetUp]
        public void SetUp()
        {
            Utilities.ConfigureLogging();
        }

        [Test]
        public void TestSingleSpill()
        {
            TestSpillRecordWriter(5, 10000, 100 * 1024, 1);
        }

        [Test]
        public void TestMultipleSpills()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6);
        }

        [Test]
        public void TestCombinerSingleSpill()
        {
            TestSpillRecordWriter(5, 10000, 100 * 1024, 1, true);
        }

        [Test]
        public void TestCombinerMultipleSpills()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6, true);
        }

        [Test]
        public void TestEmptySpill()
        {
            TestSpillRecordWriter(1, 0, 100 * 1024, 1, false);
        }

        [Test]
        public void TestCompression()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6, false, CompressionType.GZip);
        }

        [Test]
        public void TestCombinerCompression()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6, true, CompressionType.GZip);
        }

        [Test]
        public void TestCustomComparer()
        {
            TestSpillRecordWriter(5, 110000, 100 * 1024, 6, false, CompressionType.None, new ReverseComparer());
        }

        private void TestSpillRecordWriter(int partitionCount, int records, int bufferSize, int expectedSpillCount, bool useCombiner = false, CompressionType compressionType = CompressionType.None, IComparer<int> comparer = null)
        {
            List<int> values;
            if( useCombiner )
            {
                List<int> temp = Utilities.GenerateNumberData(records / 2);
                values = new List<int>(temp.Concat(temp)); // Make sure there are duplicates
            }
            else
                values = Utilities.GenerateNumberData(records);
            HashPartitioner<int> partitioner = new HashPartitioner<int>();
            partitioner.Partitions = partitionCount;
            List<int>[] expectedPartitions = new List<int>[partitionCount];
            for( int x = 0; x < partitionCount; ++x )
                expectedPartitions[x] = new List<int>();

            string outputPath = Path.Combine(Utilities.TestOutputPath, "spilloutput.tmp");
            if( File.Exists(outputPath) )
                File.Delete(outputPath);

            try
            {
                ITask<int, int> combiner = null;
                if( useCombiner )
                    combiner = new DuplicateEliminationCombiner();
                using( SortSpillRecordWriter<int> target = new SortSpillRecordWriter<int>(outputPath, partitioner, bufferSize, (int)(0.8 * bufferSize), 4096, true, compressionType, 5, comparer, combiner, 1) )
                {
                    foreach( int value in values )
                    {
                        expectedPartitions[partitioner.GetPartition(value)].Add(value);
                        target.WriteRecord(value);
                    }

                    target.FinishWriting();
                    Assert.AreEqual(expectedSpillCount, target.SpillCount);
                }

                PartitionFileIndex index = new PartitionFileIndex(outputPath);
                for( int partition = 0; partition < partitionCount; ++partition )
                {
                    IEnumerable<PartitionFileIndexEntry> entries = index.GetEntriesForPartition(partition + 1);
                    if( entries == null )
                        CollectionAssert.IsEmpty(expectedPartitions[partition]);
                    else
                    {
                        Assert.AreEqual(1, entries.Count());
                        using( PartitionFileStream stream = new PartitionFileStream(outputPath, 4096, entries, compressionType) )
                        using( BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream, 0, stream.Length, true, true) )
                        {
                            List<int> actualPartition = reader.EnumerateRecords().ToList();
                            expectedPartitions[partition].Sort(comparer);
                            if( useCombiner )
                                CollectionAssert.AreEqual(expectedPartitions[partition].Distinct().ToList(), actualPartition);
                            else
                                CollectionAssert.AreEqual(expectedPartitions[partition], actualPartition);
                        }
                    }
                }
            }
            finally
            {
                if( File.Exists(outputPath) )
                    File.Delete(outputPath);
            }
        }
    }
}
