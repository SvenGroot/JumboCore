// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Tasks;
using Ookii.Jumbo.Test.Tasks;

namespace Ookii.Jumbo.Test.Jet
{
    public abstract class JobExecutionTestsBase
    {
        #region Nested types

        protected enum TaskKind
        {
            Pull,
            Push,
            NoOutput
        }

        #endregion

        private TestJetCluster _cluster;

        private List<string> _words;
        private List<Pair<Utf8String, int>> _expectedWordCountOutput;

        protected List<int> _sortData;

        [OneTimeSetUp]
        public void Setup()
        {
            _cluster = CreateCluster();
        }


        [OneTimeTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        protected TestJetCluster Cluster
        {
            get { return _cluster; }
        }

        protected abstract TestJetCluster CreateCluster();

        protected static JobStatus RunJob(FileSystemClient fileSystemClient, JobConfiguration config, int expectedErrors = 0)
        {
            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(StringConversionTask).Assembly.Location);

            bool complete = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete);
            JobStatus status = target.JobServer.GetJobStatus(job.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(expectedErrors, status.ErrorTaskCount);
            Assert.AreEqual(config.Stages.Sum(s => s.TaskCount), status.FinishedTaskCount);

            return status;
        }

        protected void RunWordCountJob(string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue, bool mapReduce = false)
        {
            FileSystemClient client = Cluster.FileSystemClient;
            JobConfiguration config = CreateWordCountJob(client, outputPath, taskKind, channelType, forceFileDownload, maxSplitSize, mapReduce);
            RunJob(client, config);
            if (taskKind == TaskKind.NoOutput)
                VerifyEmptyWordCountOutput(client, config);
            else
                VerifyWordCountOutput(client, config);
        }

        protected JobConfiguration CreateWordCountJob(FileSystemClient client, string outputPath, TaskKind taskKind, ChannelType channelType, bool forceFileDownload, int maxSplitSize = Int32.MaxValue, bool mapReduce = false)
        {
            string inputFileName = GetTextInputFile();

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(Cluster.FileSystemClient, Cluster.JetClient);
            job.JobName = "WordCount";
            var input = job.Read(inputFileName, typeof(LineRecordReader));
            input.MaximumSplitSize = maxSplitSize;
            var words = job.Process(input, taskKind == TaskKind.NoOutput ? typeof(WordCountNoOutputTask) : (taskKind == TaskKind.Push ? typeof(WordCountPushTask) : typeof(WordCountTask)));
            words.StageId = "WordCount";
            StageOperation countedWords;
            if (mapReduce)
            {
                // Spill sort with combiner
                var sorted = job.SpillSortCombine(words, typeof(WordCountReduceTask));
                countedWords = job.Process(sorted, typeof(WordCountReduceTask));
            }
            else
            {
                countedWords = job.GroupAggregate(words, typeof(SumTask<Utf8String>));
                countedWords.InputChannel.ChannelType = channelType;
            }
            countedWords.StageId = "WordCountAggregate";
            job.Write(countedWords, outputPath, typeof(BinaryRecordWriter<>));

            JobConfiguration config = job.CreateJob();

            // Assumes that if split size is specified, it is always less than the block size.
            if (maxSplitSize < Int32.MaxValue)
                Assert.Greater(config.GetStage("WordCount").TaskCount, client.GetFileInfo(inputFileName).Blocks.Count);
            else
                Assert.AreEqual(client.GetFileInfo(inputFileName).Blocks.Count, config.GetStage("WordCount").TaskCount);

            if (forceFileDownload)
            {
                foreach (ChannelConfiguration channel in config.GetAllChannels())
                {
                    if (channel.ChannelType == ChannelType.File)
                        channel.ForceFileDownload = true;
                }
            }

            return config;
        }

        protected void RunMemorySortJob(string outputPath, ChannelType channelType, int partitionsPerTask)
        {
            FileSystemClient client = Cluster.FileSystemClient;
            JobConfiguration config = CreateMemorySortJob(client, outputPath, channelType, partitionsPerTask);
            RunJob(client, config);
            VerifySortOutput(client, config);
        }

        protected JobConfiguration CreateMemorySortJob(FileSystemClient client, string outputPath, ChannelType channelType, int partitionsPerTask)
        {
            // The primary purpose of the memory sort job here is to test internal partitioning for a compound task
            string inputFileName = GetSortInputFile(client);

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(Cluster.FileSystemClient, Cluster.JetClient);
            job.JobName = "MemorySort";

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var converted = job.Process(input, typeof(StringConversionTask));
            var sorted = job.MemorySort(converted);
            // Set spill buffer to ensure multiple spills
            if (channelType == ChannelType.Tcp)
                sorted.InputChannel.Settings.AddSetting(TcpOutputChannel.SpillBufferSizeSettingKey, "1MB");
            else
                sorted.InputChannel.Settings.AddSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferSize, "1MB");
            sorted.InputChannel.ChannelType = channelType;
            sorted.InputChannel.PartitionsPerTask = partitionsPerTask;
            job.Write(sorted, outputPath, typeof(BinaryRecordWriter<>));

            return job.CreateJob();
        }

        protected static string CreateOutputPath(FileSystemClient client, string outputPath)
        {
            if (outputPath == null)
                outputPath = "/" + TestContext.CurrentContext.Test.Name;

            client.CreateDirectory(outputPath);
            return outputPath;
        }

        protected JobStatus RunSpillSortJob(string outputPath, int partitionsPerTask, bool forceFileDownload, Type partitionerType = null)
        {
            FileSystemClient client = Cluster.FileSystemClient;
            JobConfiguration config = CreateSpillSortJob(client, outputPath, partitionsPerTask, forceFileDownload, partitionerType);
            JobStatus status = RunJob(client, config);
            VerifySortOutput(client, config, partitionerType);
            return status;
        }

        protected JobConfiguration CreateSpillSortJob(FileSystemClient client, string outputPath, int partitionsPerTask, bool forceFileDownload, Type partitionerType = null)
        {
            string inputFileName = GetSortInputFile(client);

            outputPath = CreateOutputPath(client, outputPath);

            JobBuilder job = new JobBuilder(Cluster.FileSystemClient, Cluster.JetClient);
            job.JobName = "SpillSort";

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var converted = job.Process(input, typeof(StringConversionTask));
            var sorted = job.SpillSort(converted);
            // Set spill buffer to ensure multiple spills
            sorted.InputChannel.Settings.AddSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferSize, "1MB");
            sorted.InputChannel.PartitionsPerTask = partitionsPerTask;
            sorted.InputChannel.PartitionerType = partitionerType;
            job.Write(sorted, outputPath, typeof(BinaryRecordWriter<>));

            JobConfiguration config = job.CreateJob();

            if (forceFileDownload)
            {
                foreach (ChannelConfiguration channel in config.GetAllChannels())
                {
                    if (channel.ChannelType == ChannelType.File)
                        channel.ForceFileDownload = true;
                }
            }

            return config;
        }

        protected void VerifyWordCountOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("WordCountAggregate");

            List<Pair<Utf8String, int>>[] expectedPartitions = new List<Pair<Utf8String, int>>[stage.TaskCount];
            IPartitioner<Pair<Utf8String, int>> partitioner = new HashPartitioner<Pair<Utf8String, int>>() { Partitions = stage.TaskCount };
            expectedPartitions = new List<Pair<Utf8String, int>>[stage.TaskCount];
            for (int x = 0; x < expectedPartitions.Length; ++x)
                expectedPartitions[x] = new List<Pair<Utf8String, int>>();
            if (_expectedWordCountOutput == null)
            {
                _expectedWordCountOutput = (from w in _words
                                            group w by w into g
                                            select Pair.MakePair(new Utf8String(g.Key), g.Count())).ToList();
            }
            foreach (var word in _expectedWordCountOutput)
            {
                expectedPartitions[partitioner.GetPartition(word)].Add(word);
            }

            for (int partition = 0; partition < stage.TaskCount; ++partition)
            {
                string outputFileName = FileDataOutput.GetOutputPath(stage, partition + 1);
                using (Stream stream = client.OpenFile(outputFileName))
                using (BinaryRecordReader<Pair<Utf8String, int>> reader = new BinaryRecordReader<Pair<Utf8String, int>>(stream))
                {
                    List<Pair<Utf8String, int>> actual = reader.EnumerateRecords().ToList();
                    CollectionAssert.AreEquivalent(expectedPartitions[partition], actual);
                }
            }
        }

        protected void VerifyEmptyWordCountOutput(FileSystemClient client, JobConfiguration config)
        {
            StageConfiguration stage = config.GetStage("WordCountAggregate");
            for (int partition = 0; partition < stage.TaskCount; ++partition)
            {
                string outputFileName = FileDataOutput.GetOutputPath(stage, partition + 1);
                Assert.AreEqual(0L, client.GetFileInfo(outputFileName).Size);
            }
        }

        protected void VerifySortOutput(FileSystemClient client, JobConfiguration config, Type partitionerType = null)
        {
            StageConfiguration stage = config.GetStage("MergeStage");
            int partitions = stage.TaskCount * config.GetInputStagesForStage("MergeStage").Single().OutputChannel.PartitionsPerTask;

            // Can't cache the results because the number of partitions isn't the same in each test (unlike the WordCount tests).
            IPartitioner<int> partitioner = partitionerType == null ? new HashPartitioner<int>() : (IPartitioner<int>)Activator.CreateInstance(partitionerType);
            partitioner.Partitions = partitions;
            List<int>[] expectedSortPartitions = new List<int>[partitions];
            for (int x = 0; x < partitions; ++x)
                expectedSortPartitions[x] = new List<int>();

            foreach (int value in _sortData)
            {
                expectedSortPartitions[partitioner.GetPartition(value)].Add(value);
            }

            for (int x = 0; x < partitions; ++x)
            {
                expectedSortPartitions[x].Sort();

                using (Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(stage, x + 1)))
                using (BinaryRecordReader<int> reader = new BinaryRecordReader<int>(stream))
                {
                    CollectionAssert.AreEqual(expectedSortPartitions[x], reader.EnumerateRecords().ToList());
                }
            }
        }

        protected string GetTextInputFile()
        {
            const string fileName = "/input.txt";

            if (_words == null)
            {
                FileSystemClient fileSystemClient = Cluster.FileSystemClient;
                using (Stream stream = fileSystemClient.CreateFile(fileName))
                using (StreamWriter writer = new StreamWriter(stream))
                {
                    _words = Utilities.GenerateDataWords(writer, 200000, 10);
                }
                Utilities.TraceLineAndFlush("File generation complete.");
            }

            return fileName;
        }

        protected string GetSortInputFile(FileSystemClient client)
        {
            const int recordCount = 2500000;
            const string fileName = "/sort.txt";
            if (_sortData == null)
            {
                Random rnd = new Random();

                _sortData = new List<int>();
                using (Stream stream = client.CreateFile(fileName))
                using (TextRecordWriter<int> writer = new TextRecordWriter<int>(stream))
                {
                    for (int x = 0; x < recordCount; ++x)
                    {
                        int record = rnd.Next();
                        _sortData.Add(record);
                        writer.WriteRecord(record);
                    }
                }
            }
            return fileName;
        }
    }
}
