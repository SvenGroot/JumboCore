// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class TaskTests
    {
        #region Nested types

        private class TestAccumulator : AccumulatorTask<Utf8String, int>
        {
            protected override int Accumulate(Utf8String key, int currentValue, int newValue)
            {
                return currentValue + newValue;
            }
        }

        [AllowRecordReuse]
        private class TestRecordReuseAccumulator : AccumulatorTask<Utf8String, int>
        {
            protected override int Accumulate(Utf8String key, int currentValue, int newValue)
            {
                return currentValue + newValue;
            }
        }

        private class TestReducer : ReduceTask<Utf8String, int, Pair<Utf8String, int>>
        {
            protected override void Reduce(Utf8String key, IEnumerable<int> values, RecordWriter<Pair<Utf8String, int>> output)
            {
                int sum = values.Sum();
                output.WriteRecord(Pair.MakePair(key, sum));
                // Test that the values collection is properly guarded against repeated iteration (we can't consume any more records beyond our key).
                CollectionAssert.IsEmpty(values);
            }
        }

        #endregion

        [OneTimeSetUp]
        public void SetUp()
        {
            if (Directory.Exists(Utilities.TestOutputPath))
                Directory.Delete(Utilities.TestOutputPath, true);
            Directory.CreateDirectory(Utilities.TestOutputPath);

            Utilities.ConfigureLogging();
        }

        [Test]
        public void TestSortTask()
        {
            const int recordCountMin = 1000;
            const int recordCountMax = 10000;
            Random rnd = new Random();
            int recordCount = rnd.Next(recordCountMin, recordCountMax);
            List<int> records = new List<int>(recordCount);
            for (int record = 0; record < recordCount; ++record)
            {
                int value = rnd.Next();
                records.Add(value);
            }
            ListRecordWriter<int> output = new ListRecordWriter<int>();
            MultiRecordWriter<int> multiOutput = new MultiRecordWriter<int>(new[] { output }, new PrepartitionedPartitioner<int>());
            PrepartitionedRecordWriter<int> prepartitionedOutput = new PrepartitionedRecordWriter<int>(multiOutput, false);

            SortTask<int> target = new SortTask<int>();
            target.NotifyConfigurationChanged();
            foreach (int record in records)
                target.ProcessRecord(record, 0, prepartitionedOutput);
            target.Finish(prepartitionedOutput);

            records.Sort();
            Assert.AreNotSame(records, output.List);
            Assert.IsTrue(Utilities.CompareList(records, output.List));
        }

        [Test]
        public void TestAccumulatorTask()
        {
            JobConfiguration jobConfig = new JobConfiguration();
            StageConfiguration stageConfig = jobConfig.AddStage("Accumulate", typeof(TestAccumulator), 1, null);
            TaskContext config = new TaskContext(Guid.NewGuid(), jobConfig, new TaskAttemptId(new TaskId("Accumulate", 1), 1), stageConfig, Utilities.TestOutputPath, "/JumboJet/fake");

            PushTask<Pair<Utf8String, int>, Pair<Utf8String, int>> task = new TestAccumulator();
            JetActivator.ApplyConfiguration(task, null, null, config);
            ListRecordWriter<Pair<Utf8String, int>> output = new ListRecordWriter<Pair<Utf8String, int>>(true);

            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("hello"), 1), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("bye"), 2), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("bye"), 3), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("hello"), 4), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("hello"), 5), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("bye"), 1), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("foo"), 1), output);
            task.ProcessRecord(new Pair<Utf8String, int>(new Utf8String("bye"), 1), output);

            task.Finish(output);

            var result = output.List;
            Assert.AreEqual(3, result.Count);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("hello"), 10), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("bye"), 7), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("foo"), 1), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("hello"), 10), result);
            CollectionAssert.DoesNotContain(result, new Pair<Utf8String, int>(new Utf8String("hello"), 9));
            CollectionAssert.DoesNotContain(result, new Pair<Utf8String, int>(new Utf8String("bar"), 1));
        }

        [Test]
        public void TestAccumulatorTaskRecordReuse()
        {
            JobConfiguration jobConfig = new JobConfiguration();
            StageConfiguration stageConfig = jobConfig.AddStage("Accumulate", typeof(TestAccumulator), 1, null);
            TaskContext config = new TaskContext(Guid.NewGuid(), jobConfig, new TaskAttemptId(new TaskId("Accumulate", 1), 1), stageConfig, Utilities.TestOutputPath, "/JumboJet/fake");

            PushTask<Pair<Utf8String, int>, Pair<Utf8String, int>> task = new TestRecordReuseAccumulator();
            JetActivator.ApplyConfiguration(task, null, null, config);
            ListRecordWriter<Pair<Utf8String, int>> output = new ListRecordWriter<Pair<Utf8String, int>>(true);

            Pair<Utf8String, int> record = new Pair<Utf8String, int>(new Utf8String("hello"), 1);
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 2;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 3;
            task.ProcessRecord(record, output);
            record.Key.Set("hello");
            record.Value = 4;
            task.ProcessRecord(record, output);
            record.Key.Set("hello");
            record.Value = 5;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 1;
            task.ProcessRecord(record, output);
            record.Key.Set("foo");
            record.Value = 1;
            task.ProcessRecord(record, output);
            record.Key.Set("bye");
            record.Value = 1;
            task.ProcessRecord(record, output);

            task.Finish(output);

            var result = output.List;
            ValidateOutput(result);
        }

        [Test]
        public void TestReduceTask()
        {
            TestReducer task = new TestReducer();
            task.NotifyConfigurationChanged();
            var records = CreateRecords();

            records.Sort();

            ListRecordWriter<Pair<Utf8String, int>> output = new ListRecordWriter<Pair<Utf8String, int>>();
            task.Run(new EnumerableRecordReader<Pair<Utf8String, int>>(records), output);

            ValidateOutput(output.List);
        }

        private List<Pair<Utf8String, int>> CreateRecords()
        {
            List<Pair<Utf8String, int>> records = new List<Pair<Utf8String, int>>();
            records.Add(new Pair<Utf8String, int>(new Utf8String("hello"), 1));
            records.Add(new Pair<Utf8String, int>(new Utf8String("bye"), 2));
            records.Add(new Pair<Utf8String, int>(new Utf8String("bye"), 3));
            records.Add(new Pair<Utf8String, int>(new Utf8String("hello"), 4));
            records.Add(new Pair<Utf8String, int>(new Utf8String("hello"), 5));
            records.Add(new Pair<Utf8String, int>(new Utf8String("bye"), 1));
            records.Add(new Pair<Utf8String, int>(new Utf8String("foo"), 1));
            records.Add(new Pair<Utf8String, int>(new Utf8String("bye"), 1));

            return records;
        }

        private static void ValidateOutput(System.Collections.ObjectModel.ReadOnlyCollection<Pair<Utf8String, int>> result)
        {
            Assert.AreEqual(3, result.Count);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("hello"), 10), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("bye"), 7), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("foo"), 1), result);
            Assert.Contains(new Pair<Utf8String, int>(new Utf8String("hello"), 10), result);
            CollectionAssert.DoesNotContain(result, new Pair<Utf8String, int>(new Utf8String("hello"), 9));
            CollectionAssert.DoesNotContain(result, new Pair<Utf8String, int>(new Utf8String("bar"), 1));
        }

    }
}
