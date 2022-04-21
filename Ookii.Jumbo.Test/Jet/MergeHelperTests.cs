// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class MergeHelperTests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            Utilities.ConfigureLogging();
        }

        [Test]
        public void TestMerge()
        {
            TestMergeCore(5, 5, 100, 50, false, 1);
        }

        [Test]
        public void TestMergeMultiplePasses()
        {
            TestMergeCore(12, 5, 100, 50, false, 3);
        }

        [Test]
        public void TestMergeRaw()
        {
            TestMergeCore(5, 5, 100, 50, true, 1);
        }

        [Test]
        public void TestMergeRawMultiplePasses()
        {
            TestMergeCore(12, 5, 100, 50, true, 3);
        }

        private void TestMergeCore(int diskSegmentCount, int memorySegmentCount, int segmentItemCount, int segmentItemCountRandomization, bool rawComparer, int expectedPasses)
        {
            var diskSegmentData = GenerateSegmentData(diskSegmentCount, segmentItemCount, segmentItemCountRandomization);
            var diskSegments = GenerateSegments(diskSegmentData, false, rawComparer);
            var memorySegmentData = GenerateSegmentData(memorySegmentCount, segmentItemCount, segmentItemCountRandomization);
            var memorySegments = GenerateSegments(memorySegmentData, true, rawComparer);

            var expected = diskSegmentData.SelectMany(s => s).Concat(memorySegmentData.SelectMany(s => s)).OrderBy(s => s).ToList();

            var target = new MergeHelper<int>();
            var actual = target.Merge(diskSegments, memorySegments, 5, null, false, Utilities.TestOutputPath, CompressionType.None, 4096, true).Select(r => r.GetValue()).ToList();

            CollectionAssert.AreEqual(expected, actual);
            Assert.AreEqual(expectedPasses, target.MergePassCount);
            if (rawComparer)
            {
                if (expectedPasses == 1)
                    Assert.AreEqual(0, target.BytesWritten);
                else
                {
                    Assert.AreNotEqual(0, target.BytesWritten);
                    Assert.Greater(target.BytesRead, target.BytesWritten);
                }
                Assert.AreNotEqual(0, target.BytesRead); // Bytes read by MemoryStream
            }
            else
            {
                if (expectedPasses == 1)
                    Assert.AreEqual(0, target.BytesRead); // No bytes read by EnumerableComparer.
                else
                    Assert.AreNotEqual(0, target.BytesRead);
                Assert.AreEqual(target.BytesRead, target.BytesWritten);
            }
        }

        private List<List<int>> GenerateSegmentData(int segmentCount, int itemCount, int itemCountRandomization)
        {
            List<List<int>> result = new List<List<int>>();
            Random rnd = new Random();
            for (int x = 0; x < segmentCount; ++x)
            {
                List<int> segment = Utilities.GenerateNumberData(itemCount + rnd.Next(itemCountRandomization), rnd);
                segment.Sort();
                result.Add(segment);
            }
            return result;
        }

        private List<RecordInput> GenerateSegments(List<List<int>> segments, bool isMemoryBased, bool serialize)
        {
            if (serialize)
            {
                List<RecordInput> result = new List<RecordInput>();
                foreach (List<int> segment in segments)
                {
                    MemoryStream stream = new MemoryStream();
                    BinaryWriter writer = new BinaryWriter(stream);
                    foreach (int value in segment)
                    {
                        WritableUtility.Write7BitEncodedInt32(writer, sizeof(int));
                        writer.Write(value);
                    }
                    stream.Position = 0;
                    result.Add(new StreamRecordInput(typeof(BinaryRecordReader<int>), stream, isMemoryBased, null, true, false));
                }
                return result;
            }
            else
                return segments.Select(s => (RecordInput)new ReaderRecordInput(new EnumerableRecordReader<int>(s), isMemoryBased)).ToList();
        }
    }
}
