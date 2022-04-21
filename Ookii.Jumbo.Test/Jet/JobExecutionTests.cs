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
    [TestFixture]
    [Category("JetClusterTests")]
    public class JobExecutionTests : JobExecutionTestsBase
    {
        [Test]
        public void TestJobAbort()
        {
            FileSystemClient fileSystemClient = Cluster.FileSystemClient;

            JobConfiguration config = CreateWordCountJob(fileSystemClient, null, TaskKind.Pull, ChannelType.File, false);

            JetClient target = new JetClient(TestJetCluster.CreateClientConfig());
            Job job = target.RunJob(config, fileSystemClient, typeof(LineCounterTask).Assembly.Location);

            JobStatus status;
            do
            {
                Thread.Sleep(1000);
                status = target.JobServer.GetJobStatus(job.JobId);
            } while (status.RunningTaskCount == 0);
            Thread.Sleep(1000);
            target.JobServer.AbortJob(job.JobId);
            bool finished = target.WaitForJobCompletion(job.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(finished);
            Assert.IsFalse(target.JobServer.GetJobStatus(job.JobId).IsSuccessful);
            Thread.Sleep(5000);
        }

        [Test]
        public void TestWordCount()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountPushTask()
        {
            RunWordCountJob(null, TaskKind.Push, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountNoIntermediateData()
        {
            RunWordCountJob(null, TaskKind.NoOutput, ChannelType.File, false);
        }

        [Test]
        public void TestWordCountNoIntermediateDataFileChannelDownload()
        {
            RunWordCountJob(null, TaskKind.NoOutput, ChannelType.File, true);
        }

        [Test]
        public void TestWordCountFileChannelDownload()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, true);
        }

        [Test]
        public void TestWordCountTcpChannel()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.Tcp, false);
        }

        [Test]
        public void TestWordCountMaxSplitSize()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false, Cluster.FileSystemClient.DefaultBlockSize.Value / 2);
        }

        [Test]
        public void TestWordCountMapReduce()
        {
            RunWordCountJob(null, TaskKind.Pull, ChannelType.File, false, mapReduce: true);
        }

        [Test]
        public void TestMemorySort()
        {
            RunMemorySortJob(null, ChannelType.File, 1);
        }

        [Test]
        public void TestMemorySortTcpChannel()
        {
            RunMemorySortJob(null, ChannelType.Tcp, 1);
        }

        [Test]
        public void TestMemorySortTcpChannelMultiplePartitionsPerTask()
        {
            RunMemorySortJob(null, ChannelType.Tcp, 3);
        }

        [Test]
        public void TestSpillSort()
        {
            RunSpillSortJob(null, 1, false);
        }

        [Test]
        public void TestSpillSortFileChannelDownload()
        {
            RunSpillSortJob(null, 1, true);
        }

        [Test]
        public void TestSpillSortMultiplePartitionsPerTask()
        {
            RunSpillSortJob(null, 3, false);
        }

        [Test]
        public void TestSpillSortDynamicPartitionAssignment()
        {
            // The skewed partitioner will assign most data to the first partition. This will cause task 2 to take over most of task 1's partitions.
            JobStatus status = RunSpillSortJob(null, 10, true, typeof(SkewedPartitioner<int>));
            StageStatus stage = status.Stages.Where(s => s.StageId == "MergeStage").Single();
            Assert.Greater(stage.Metrics.DynamicallyAssignedPartitions, 0);
            Assert.Greater(stage.Metrics.DiscardedPartitions, 0);
        }

        [Test]
        public void TestSpillSortFileChannelDownloadMultiplePartitionsPerTask()
        {
            RunSpillSortJob(null, 3, true);
        }

        [Test]
        public void TestJobSettings()
        {
            FileSystemClient client = Cluster.FileSystemClient;

            string inputFile = GetSortInputFile(client);
            string outputPath = CreateOutputPath(client, null);

            JobBuilder job = new JobBuilder(client, Cluster.JetClient);
            var input = job.Read(inputFile, typeof(LineRecordReader));
            var multiplied = job.Process(input, typeof(MultiplierTask));
            job.Write(multiplied, outputPath, typeof(TextRecordWriter<>));
            int factor = new Random().Next(2, 100);
            job.Settings.AddSetting("factor", factor);

            JobConfiguration config = job.CreateJob();
            RunJob(client, config);

            StageConfiguration stage = config.GetStage("MultiplierTaskStage");

            List<int> expected = _sortData.Select(value => value * factor).ToList();
            List<int> actual = new List<int>();
            for (int x = 0; x < stage.TaskCount; ++x)
            {
                using (Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(stage, x + 1)))
                using (LineRecordReader reader = new LineRecordReader(stream))
                {
                    actual.AddRange(reader.EnumerateRecords().Select(r => Convert.ToInt32(r.ToString())));
                }
            }

            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        public void TestInnerJoin()
        {
            FileSystemClient client = Cluster.FileSystemClient;
            List<Customer> customers = new List<Customer>();
            List<Order> orders = new List<Order>();

            GenerateJoinData(client, customers, orders);

            client.CreateDirectory("/testjoinoutput");

            JobBuilder job = new JobBuilder(client, Cluster.JetClient);
            var customerInput = job.Read("/testjoin/customers", typeof(RecordFileReader<Customer>));
            var orderInput = job.Read("/testjoin/orders", typeof(RecordFileReader<Order>));
            var joined = job.InnerJoin(customerInput, orderInput, typeof(CustomerOrderJoinRecordReader), null, typeof(OrderJoinComparer));
            job.Write(joined, "/testjoinoutput", typeof(RecordFileWriter<>));

            JobConfiguration config = job.CreateJob();

            RunJob(client, config);

            List<CustomerOrder> actual = new List<CustomerOrder>();
            StageConfiguration stage = config.GetStage("JoinStage");
            for (int x = 0; x < stage.TaskCount; ++x)
            {
                using (Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(stage, x + 1)))
                using (RecordFileReader<CustomerOrder> reader = new RecordFileReader<CustomerOrder>(stream))
                {
                    actual.AddRange(reader.EnumerateRecords());
                }
            }

            List<CustomerOrder> expected = (from customer in customers
                                            join order in orders on customer.Id equals order.CustomerId
                                            select new CustomerOrder() { CustomerId = customer.Id, ItemId = order.ItemId, Name = customer.Name, OrderId = order.Id }).ToList();

            // AreEquivalent is too slow.
            expected.Sort();
            actual.Sort();
            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        public void TestTaskTimeout()
        {
            FileSystemClient client = Cluster.FileSystemClient;
            JobConfiguration config = CreateWordCountJob(client, null, TaskKind.Pull, ChannelType.File, false);
            // Only the first task attempt will delay, so that will be killed. The job should still succeed afterwards
            config.AddTypedSetting(WordCountTask.DelayTimeSettingKey, 6000000);
            config.AddTypedSetting(TaskServerConfigurationElement.TaskTimeoutJobSettingKey, 20000); // Set timeout to 20 seconds.

            JobStatus status = RunJob(client, config, 1);

            Assert.AreEqual(2, status.Stages.Where(s => s.StageId == "WordCount").Single().Tasks[0].Attempts);

            VerifyWordCountOutput(client, config);
        }

        [Test]
        public void TestHardDependency()
        {
            FileSystemClient client = Cluster.FileSystemClient;
            string inputFileName = GetTextInputFile();
            string outputPath = CreateOutputPath(client, null);
            string verificationOutputPath = outputPath + "Verify";
            client.CreateDirectory(verificationOutputPath);

            JobBuilder job = new JobBuilder(Cluster.FileSystemClient, Cluster.JetClient);
            job.JobName = "WordCount";

            // Creating this operation first; it should not get scheduled first because of the dependency
            // The OutputVerificationTask will write true to the output only if all the output of the dependent stage already exists.
            var verification = job.Generate(1, typeof(OutputVerificationTask));
            job.Write(verification, verificationOutputPath, typeof(BinaryRecordWriter<>));

            var input = job.Read(inputFileName, typeof(LineRecordReader));
            var words = job.Process(input, typeof(WordCountTask));
            words.StageId = "WordCount";
            var countedWords = job.GroupAggregate(words, typeof(SumTask<Utf8String>));
            countedWords.StageId = "WordCountAggregate";
            job.Write(countedWords, outputPath, typeof(BinaryRecordWriter<>));

            verification.AddSchedulingDependency(countedWords);

            JobConfiguration config = job.CreateJob();

            config.AddSetting(OutputVerificationTask.StageToVerifySettingName, "WordCountAggregate");

            RunJob(client, config);

            VerifyWordCountOutput(client, config);

            using (Stream stream = client.OpenFile(FileDataOutput.GetOutputPath(config.GetStage("OutputVerificationTaskStage"), 1)))
            using (BinaryRecordReader<bool> reader = new BinaryRecordReader<bool>(stream))
            {
                bool actual = false;
                if (reader.ReadRecord())
                    actual = reader.CurrentRecord;
                Assert.IsTrue(actual);
            }
        }

        [Test]
        public void TestMultipleSimultaneousJobs()
        {
            const string outputPath1 = "/multiple1";
            const string outputPath2 = "/multiple2";
            FileSystemClient fileSystemClient = Cluster.FileSystemClient;

            JobConfiguration config1 = CreateWordCountJob(fileSystemClient, outputPath1, TaskKind.Pull, ChannelType.File, false);
            JobConfiguration config2 = CreateWordCountJob(fileSystemClient, outputPath2, TaskKind.Pull, ChannelType.File, false);

            JetClient target = Cluster.JetClient;
            Job job1 = target.RunJob(config1, fileSystemClient, typeof(WordCountTask).Assembly.Location);
            Job job2 = target.RunJob(config2, fileSystemClient, typeof(WordCountTask).Assembly.Location);

            bool complete1 = target.WaitForJobCompletion(job1.JobId, Timeout.Infinite, 1000);
            bool complete2 = target.WaitForJobCompletion(job2.JobId, Timeout.Infinite, 1000);
            Assert.IsTrue(complete1);
            Assert.IsTrue(complete2);
            JobStatus status = target.JobServer.GetJobStatus(job1.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);
            status = target.JobServer.GetJobStatus(job2.JobId);
            Assert.IsTrue(status.IsSuccessful);
            Assert.AreEqual(0, status.ErrorTaskCount);

            VerifyWordCountOutput(fileSystemClient, config1);
            VerifyWordCountOutput(fileSystemClient, config2);
        }

        protected override TestJetCluster CreateCluster()
        {
            return new TestJetCluster(16777216, true, 2, CompressionType.None);
        }


        private void GenerateJoinData(FileSystemClient fileSystemClient, List<Customer> customers, List<Order> orders)
        {
            Utilities.GenerateJoinData(customers, orders, 30000, 3, 100);
            customers.Randomize();
            orders.Randomize();

            fileSystemClient.CreateDirectory("/testjoin");
            using (Stream stream = fileSystemClient.CreateFile("/testjoin/customers"))
            using (RecordFileWriter<Customer> recordFile = new RecordFileWriter<Customer>(stream))
            {
                recordFile.WriteRecords(customers);
            }

            using (Stream stream = fileSystemClient.CreateFile("/testjoin/orders"))
            using (RecordFileWriter<Order> recordFile = new RecordFileWriter<Order>(stream))
            {
                recordFile.WriteRecords(orders);
            }
        }
    }
}
