// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class MergeRecordReaderTests
    {
        [OneTimeSetUp]
        public void SetUp()
        {
            if( Directory.Exists(Utilities.TestOutputPath) )
                Directory.Delete(Utilities.TestOutputPath, true);
            Directory.CreateDirectory(Utilities.TestOutputPath);

            Utilities.ConfigureLogging();
        }

        [Test]
        public void TestMergeRecordReader()
        {
            TestMergeSort(1, 0, 100, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMemoryInputs()
        {
            TestMergeSort(1, 0, 100, CompressionType.None, false, true);
        }

        [Test]
        public void TestMergeRecordReaderMemoryInputsPurge()
        {
            TestMergeSort(1, 0, 100, CompressionType.None, true, true);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePasses()
        {
            TestMergeSort(1, 0, 20, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePassesWithCompression()
        {
            TestMergeSort(1, 0, 20, CompressionType.GZip);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePartitions()
        {
            TestMergeSort(3, 0, 100, CompressionType.None);
        }

        [Test]
        public void TestMergeRecordReaderMultiplePartitionsMultiplePasses()
        {
            TestMergeSort(3, 0, 20, CompressionType.None);
        }

        [Test]
        public void TestAssignAdditionalPartitions()
        {
            TestMergeSort(3, 2, 100, CompressionType.None);
        }

        [Test]
        public void TestAssignAdditionalPartitionsMultiplePasses()
        {
            TestMergeSort(3, 2, 20, CompressionType.None);
        }

        private static void TestMergeSort(int partitions, int extraPartitionGroups, int maxMergeInputs, CompressionType compression, bool purgeMemory = false, bool memoryInputs = false)
        {
            const int inputCount = 50;
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            MergeRecordReader<int> reader = new MergeRecordReader<int>(Enumerable.Range(0, partitions), inputCount, false, 4096, compression);
            StageConfiguration stageConfig = new StageConfiguration();
            stageConfig.AddSetting(MergeRecordReaderConstants.MaxFileInputsSetting, maxMergeInputs);
            stageConfig.AddSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, purgeMemory);
            stageConfig.StageId = "Merge";
            reader.JetConfiguration = new JetConfiguration();
            reader.TaskContext = new TaskContext(Guid.Empty, new JobConfiguration(), new TaskAttemptId(new TaskId(stageConfig.StageId, 1), 1), stageConfig, Utilities.TestOutputPath, "");
            reader.NotifyConfigurationChanged();
            Random rnd = new Random();
            const int partitionGroupSize = 2;
            List<int>[] sortedLists = new List<int>[partitions + extraPartitionGroups * partitionGroupSize];
            RecordInput[] partitionInputs = new RecordInput[partitions];
            for( int x = 0; x < inputCount; ++x )
            {
                for( int partition = 0; partition < partitions; ++partition )
                {
                    CreatePartition(recordCountMin, recordCountMax, rnd, sortedLists, partitionInputs, partition, 0, memoryInputs);
                }
                reader.AddInput(partitionInputs);
            }

            for( int partition = 0; partition < partitions; ++partition, reader.NextPartition() )
            {
                List<int> expected = sortedLists[partition];
                expected.Sort();

                List<int> result = new List<int>(reader.EnumerateRecords());

                CollectionAssert.AreEqual(expected, result);
            }

            partitionInputs = new RecordInput[partitionGroupSize];
            for( int group = 0; group < extraPartitionGroups; ++group )
            {
                int firstPartition = partitions + group * partitionGroupSize;
                reader.AssignAdditionalPartitions(Enumerable.Range(firstPartition, partitionGroupSize).ToList());
                reader.NextPartition();
                for( int input = 0; input < inputCount; ++input )
                {
                    for( int partition = 0; partition < partitionGroupSize; ++partition )
                    {
                        CreatePartition(recordCountMin, recordCountMax, rnd, sortedLists, partitionInputs, partition, firstPartition, memoryInputs);
                    }
                    reader.AddInput(partitionInputs);
                }

                for( int partition = firstPartition; partition < firstPartition + partitionGroupSize; ++partition, reader.NextPartition() )
                {
                    List<int> expected = sortedLists[partition];
                    expected.Sort();

                    List<int> result = new List<int>(reader.EnumerateRecords());

                    CollectionAssert.AreEqual(expected, result);
                }
            }
        }

        private static void CreatePartition(int recordCountMin, int recordCountMax, Random rnd, List<int>[] sortedLists, RecordInput[] partitionInputs, int partition, int firstPartition, bool memoryInputs)
        {
            if( sortedLists[firstPartition + partition] == null )
                sortedLists[firstPartition + partition] = new List<int>();
            int recordCount = rnd.Next(recordCountMin, recordCountMax);
            List<int> records = new List<int>(recordCount);
            for( int record = 0; record < recordCount; ++record )
            {
                int value = rnd.Next();
                records.Add(value);
                sortedLists[firstPartition + partition].Add(value);
            }
            records.Sort();
            partitionInputs[partition] = new ReaderRecordInput(new EnumerableRecordReader<int>(records), memoryInputs);
        }
    }
}
