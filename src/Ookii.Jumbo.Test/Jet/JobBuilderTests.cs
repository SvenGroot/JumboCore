// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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

namespace Ookii.Jumbo.Test.Jet;

[TestFixture]
public class JobBuilderTests
{
    private TestJetCluster _cluster;
    private FileSystemClient _fileSystemClient;
    private JetClient _jetClient;

    private const string _inputPath = "/test.txt";
    private const string _inputPath2 = "/test2.txt";
    private const string _outputPath = "/output";
    private const int _blockSize = 4194304;

    private static readonly HashSet<string> _allowedAssemblyNames = new HashSet<string>(new string[] {
        "Ookii.Jumbo.Test.Tasks.dll"
    });



    [OneTimeSetUp]
    public void SetUp()
    {
        _cluster = new TestJetCluster(_blockSize, true, 2, CompressionType.None);
        _fileSystemClient = _cluster.FileSystemClient;
        _jetClient = new JetClient(TestJetCluster.CreateClientConfig());
        Trace.WriteLine("Cluster running.");

        // This file will purely be used so we have something to use as input when creating jobs, it won't be read so the contents don't matter.
        using (Stream stream = _fileSystemClient.CreateFile(_inputPath))
        {
            Utilities.GenerateData(stream, 10000000);
        }
        using (Stream stream = _fileSystemClient.CreateFile(_inputPath2))
        {
            Utilities.GenerateData(stream, 10000000);
        }
        _fileSystemClient.CreateDirectory("/output");
    }


    [OneTimeTearDown]
    public void Teardown()
    {
        Trace.WriteLine("Shutting down cluster.");
        _cluster.Shutdown();
        Trace.WriteLine("Cluster shut down.");
    }

    [Test]
    public void TestProcessSingleStage()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var operation = builder.Process(input, typeof(LineCounterTask));
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(1));
        Assert.That(config.AssemblyFileNames[0], Is.EqualTo(Path.GetFileName(typeof(LineCounterTask).Assembly.Location)));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        VerifyStage(stage, 3, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
        VerifyDataInput(config, stage, typeof(LineRecordReader));
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
    }

    [Test]
    public void TestProcessMultiStage()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var op1 = builder.Process(input, typeof(LineCounterTask));
        var op2 = builder.Process(op1, typeof(LineAdderTask));
        builder.Write(op2, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(1));
        Assert.That(config.AssemblyFileNames[0], Is.EqualTo(Path.GetFileName(typeof(LineCounterTask).Assembly.Location)));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
        VerifyStage(config.Stages[1], 2, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<int>));
        config.Validate();
    }

    [Test]
    public void TestProcessDelegate()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var operation = builder.Process<Utf8String, int>(input, TaskMethods.ProcessRecords);
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(operation.TaskType.TaskType.Name, Is.EqualTo("ProcessRecordsTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        VerifyStage(stage, 3, "ProcessRecordsTaskStage", operation.TaskType.TaskType);
        VerifyDataInput(config, stage, typeof(LineRecordReader));
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestProcessDelegateNoContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var operation = builder.Process<Utf8String, int>(input, TaskMethods.ProcessRecordsNoContext);
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(operation.TaskType.TaskType.Name, Is.EqualTo("ProcessRecordsNoContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        VerifyStage(stage, 3, "ProcessRecordsNoContextTaskStage", operation.TaskType.TaskType);
        VerifyDataInput(config, stage, typeof(LineRecordReader));
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestCustomDataOutput()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var operation = builder.Process(input, typeof(LineCounterTask));
        var output = builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));
        output.BlockSize = 256 << 20;
        output.ReplicationFactor = 2;

        JobConfiguration config = builder.CreateJob();
        VerifyDataOutput(config.Stages[0], typeof(TextRecordWriter<int>), 256 << 20, 2);
        config.Validate();
    }

    [Test]
    public void TestCustomChannel()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var op1 = builder.Process(input, typeof(LineCounterTask));
        var op2 = builder.Process(op1, typeof(LineAdderTask));
        op2.InputChannel.ChannelType = ChannelType.Tcp;
        op2.InputChannel.TaskCount = 4;
        op2.InputChannel.PartitionsPerTask = 2;
        op2.InputChannel.PartitionerType = typeof(FakePartitioner<>);
        op2.InputChannel.PartitionAssignmentMethod = PartitionAssignmentMethod.Striped;
        builder.Write(op2, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();

        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.Tcp, typeof(FakePartitioner<int>), typeof(RoundRobinMultiInputRecordReader<int>), 2, PartitionAssignmentMethod.Striped);
        VerifyStage(config.Stages[1], 4, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
        config.Validate();
    }

    [Test]
    public void TestSortDataInputOutput()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.MemorySort(input);
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
        VerifyStage(config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<Utf8String>));
        VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
        VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.SortTaskComparerSettingKey, null);
        VerifyStageSetting(config.Stages[1], TaskConstants.SortTaskComparerSettingKey, null);
        config.Validate();
    }

    [Test]
    public void TestSortDataInputOutputSinglePartition()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.MemorySort(input);
        sort.InputChannel.PartitionCount = 1;
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        // EmptyTask will have been replaced because there is only one partition.
        VerifyStage(config.Stages[0], 3, "SortStage", typeof(SortTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
        VerifyStage(config.Stages[1], 1, "MergeStage", typeof(EmptyTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
        VerifyStageSetting(config.Stages[0], TaskConstants.SortTaskComparerSettingKey, null);
        VerifyStageSetting(config.Stages[1], TaskConstants.SortTaskComparerSettingKey, null);
        config.Validate();
    }

    [Test]
    public void TestSortChannelInputOutput()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);


        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var converted = builder.Process(input, typeof(StringConversionTask));
        var sorted = builder.MemorySort(converted);
        var added = builder.Process(sorted, typeof(LineAdderTask)); // Yeah, this is not a sensible job, so what?
        builder.Write(added, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(1));
        Assert.That(config.AssemblyFileNames[0], Is.EqualTo(Path.GetFileName(typeof(LineCounterTask).Assembly.Location)));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, typeof(StringConversionTask).Name + "Stage", typeof(StringConversionTask));
        VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
        VerifyStage(config.Stages[0].ChildStage, 2, "SortStage", typeof(SortTask<int>));
        VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<int>));
        // EmptyTask on second step replaced with LineAdderTask.
        VerifyStage(config.Stages[1], 2, typeof(LineAdderTask).Name + "Stage", typeof(LineAdderTask));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<int>));
        VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.SortTaskComparerSettingKey, null);
        VerifyStageSetting(config.Stages[1], TaskConstants.SortTaskComparerSettingKey, null);
        config.Validate();
    }

    [Test]
    public void TestSortCustomComparer()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.MemorySort(input, typeof(FakeComparer<>));
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.Not.EqualTo(0)); // Will contain lots of stuff because FakeComparer is in the test assembly, not the test tasks assembly.
        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.SortTaskComparerSettingKey, typeof(FakeComparer<Utf8String>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], TaskConstants.SortTaskComparerSettingKey, null);
        config.Validate();
    }

    [Test]
    public void TestSpillSort()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.SpillSort(input);
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, null);
        config.Validate();
    }

    [Test]
    public void TestSpillSortCustomComparer()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.SpillSort(input, typeof(FakeRawComparer<>));
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, typeof(FakeRawComparer<Utf8String>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, null);
        VerifyStageSetting(config.Stages[0], TaskConstants.SortTaskComparerSettingKey, null);
        VerifyStageSetting(config.Stages[1], TaskConstants.SortTaskComparerSettingKey, null);
        config.Validate();
    }

    [Test]
    public void TestSpillSortCombiner()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var sort = builder.SpillSortCombine(input, typeof(FakeCombiner<>));
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Utf8String>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Utf8String>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortCombinerType, typeof(FakeCombiner<Utf8String>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.Stage.SpillSortCombinerType, null);
        config.Validate();
    }

    [Test]
    public void TestSpillSortCombinerDelegate()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
        var sort = builder.SpillSortCombine<Utf8String, int>(input, TaskMethods.CombineRecords);
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(sort.CombinerType.Name, Is.EqualTo("CombineRecordsTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Pair<Utf8String, int>>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Pair<Utf8String, int>>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortCombinerType, sort.CombinerType.AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.Stage.SpillSortCombinerType, null);
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestSpillSortCombinerDelegateNoContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
        var sort = builder.SpillSortCombine<Utf8String, int>(input, TaskMethods.CombineRecordsNoContext);
        builder.Write(sort, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(sort.CombinerType.Name, Is.EqualTo("CombineRecordsNoContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[0], 3, "ReadStage", typeof(EmptyTask<Pair<Utf8String, int>>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[1], 2, "MergeStage", typeof(EmptyTask<Pair<Utf8String, int>>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortCombinerType, sort.CombinerType.AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.Stage.SpillSortCombinerType, null);
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestGroupAggregateDataInputOutput()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
        var aggregated = builder.GroupAggregate(input, typeof(SumTask<>));
        builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[0], 3, "Local" + typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
        VerifyStage(config.Stages[1], 2, typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        config.Validate();
    }

    [Test]
    public void TestGroupAggregateChannelInput()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var paired = builder.Process(input, typeof(GenerateInt32PairTask<>));
        var aggregated = builder.GroupAggregate(paired, typeof(SumTask<>));
        builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, typeof(GenerateInt32PairTask<Utf8String>).Name + "Stage", typeof(GenerateInt32PairTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
        VerifyStage(config.Stages[0].ChildStage, 1, "Local" + typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File);
        VerifyStage(config.Stages[1], 2, typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        config.Validate();
    }

    [Test]
    public void TestGroupAggregateCustomComparer()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var paired = builder.Process(input, typeof(GenerateInt32PairTask<>));
        var aggregated = builder.GroupAggregate(paired, typeof(SumTask<>), typeof(FakeEqualityComparer<>));
        builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(0));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, typeof(GenerateInt32PairTask<Utf8String>).Name + "Stage", typeof(GenerateInt32PairTask<Utf8String>));
        VerifyChannel(config.Stages[0], config.Stages[0].ChildStage, ChannelType.Pipeline);
        VerifyStageSetting(config.Stages[0], TaskConstants.AccumulatorTaskKeyComparerSettingKey, null);
        VerifyStageSetting(config.Stages[0], PartitionerConstants.EqualityComparerSetting, null);

        VerifyStage(config.Stages[0].ChildStage, 1, "Local" + typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyChannel(config.Stages[0].ChildStage, config.Stages[1], ChannelType.File);
        VerifyStageSetting(config.Stages[0].ChildStage, TaskConstants.AccumulatorTaskKeyComparerSettingKey, typeof(FakeEqualityComparer<Utf8String>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[0].ChildStage, PartitionerConstants.EqualityComparerSetting, typeof(FakeEqualityComparer<Utf8String>).AssemblyQualifiedName);

        VerifyStage(config.Stages[1], 2, typeof(SumTask<Utf8String>).Name + "Stage", typeof(SumTask<Utf8String>));
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        VerifyStageSetting(config.Stages[1], TaskConstants.AccumulatorTaskKeyComparerSettingKey, typeof(FakeEqualityComparer<Utf8String>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], PartitionerConstants.EqualityComparerSetting, null);
        config.Validate();
    }

    [Test]
    public void TestGroupAggregateDelegate()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
        var aggregated = builder.GroupAggregate<Utf8String, int>(input, TaskMethods.AccumulateRecords);
        builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(aggregated.TaskType.TaskType.Name, Is.EqualTo("AccumulateRecordsTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[0], 3, "LocalAccumulateRecordsTaskStage", aggregated.TaskType.TaskType);
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
        VerifyStage(config.Stages[1], 2, "AccumulateRecordsTaskStage", aggregated.TaskType.TaskType);
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestGroupAggregateDelegateNoContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(RecordFileReader<Pair<Utf8String, int>>));
        var aggregated = builder.GroupAggregate<Utf8String, int>(input, TaskMethods.AccumulateRecordsNoContext);
        builder.Write(aggregated, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(aggregated.TaskType.TaskType.Name, Is.EqualTo("AccumulateRecordsNoContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<Pair<Utf8String, int>>));
        VerifyStage(config.Stages[0], 3, "LocalAccumulateRecordsNoContextTaskStage", aggregated.TaskType.TaskType);
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File);
        VerifyStage(config.Stages[1], 2, "AccumulateRecordsNoContextTaskStage", aggregated.TaskType.TaskType);
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<Pair<Utf8String, int>>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestMapReduce()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        // This is it: the official way to write a "behaves like Hadoop" MapReduce job.
        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var mapped = builder.Map<Utf8String, Pair<Utf8String, int>>(input, TaskMethods.MapRecords);
        var sorted = builder.SpillSort(mapped);
        var reduced = builder.Reduce<Utf8String, int, int>(sorted, TaskMethods.ReduceRecords);
        builder.Write(reduced, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(mapped.TaskType.TaskType.Name, Is.EqualTo("MapRecordsTask"));
        Assert.That(reduced.TaskType.TaskType.Name, Is.EqualTo("ReduceRecordsTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));
        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "MapRecordsTaskStage", mapped.TaskType.TaskType);
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStage(config.Stages[1], 2, "ReduceRecordsTaskStage", reduced.TaskType.TaskType);
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestMapReduceNoContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var input = builder.Read(_inputPath, typeof(LineRecordReader));
        var mapped = builder.Map<Utf8String, Pair<Utf8String, int>>(input, TaskMethods.MapRecordsNoContext);
        var sorted = builder.SpillSort(mapped);
        var reduced = builder.Reduce<Utf8String, int, int>(sorted, TaskMethods.ReduceRecordsNoContext);
        builder.Write(reduced, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(mapped.TaskType.TaskType.Name, Is.EqualTo("MapRecordsNoContextTask"));
        Assert.That(reduced.TaskType.TaskType.Name, Is.EqualTo("ReduceRecordsNoContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(2));
        VerifyDataInput(config, config.Stages[0], typeof(LineRecordReader));
        VerifyStage(config.Stages[0], 3, "MapRecordsNoContextTaskStage", mapped.TaskType.TaskType);
        VerifyChannel(config.Stages[0], config.Stages[1], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<Pair<Utf8String, int>>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStage(config.Stages[1], 2, "ReduceRecordsNoContextTaskStage", reduced.TaskType.TaskType);
        VerifyDataOutput(config.Stages[1], typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestGenerate()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var operation = builder.Generate(5, typeof(LineCounterTask)); // This task actually requires input but since no one's running it, we don't care.
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        Assert.That(config.AssemblyFileNames.Count, Is.EqualTo(1));
        Assert.That(config.AssemblyFileNames[0], Is.EqualTo(Path.GetFileName(typeof(LineCounterTask).Assembly.Location)));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        Assert.That(stage.DataInput, Is.Null);
        Assert.That(config.GetInputStagesForStage(stage.StageId), Is.Empty);
        VerifyStage(stage, 5, typeof(LineCounterTask).Name + "Stage", typeof(LineCounterTask));
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
    }

    [Test]
    public void TestGenerateDelegate()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var operation = builder.Generate<int>(5, TaskMethods.GenerateRecords);
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(operation.TaskType.TaskType.Name, Is.EqualTo("GenerateRecordsTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        Assert.That(stage.DataInput, Is.Null);
        Assert.That(config.GetInputStagesForStage(stage.StageId), Is.Empty);
        VerifyStage(stage, 5, "GenerateRecordsTaskStage", operation.TaskType.TaskType);
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestGenerateDelegateProgressContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var operation = builder.Generate<int>(5, TaskMethods.GenerateRecordsProgressContext);
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(operation.TaskType.TaskType.Name, Is.EqualTo("GenerateRecordsProgressContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        Assert.That(stage.DataInput, Is.Null);
        Assert.That(config.GetInputStagesForStage(stage.StageId), Is.Empty);
        VerifyStage(stage, 5, "GenerateRecordsProgressContextTaskStage", operation.TaskType.TaskType);
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestGenerateDelegateNoContext()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var operation = builder.Generate<int>(5, TaskMethods.GenerateRecordsNoContext);
        builder.Write(operation, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();
        VerifyAssemblyNames(config.AssemblyFileNames);
        Assert.That(config.AssemblyFileNames.Last(), Does.StartWith("Ookii.Jumbo.Jet.Generated."));
        Assert.That(operation.TaskType.TaskType.Name, Is.EqualTo("GenerateRecordsNoContextTask"));

        Assert.That(config.Stages.Count, Is.EqualTo(1));
        StageConfiguration stage = config.Stages[0];
        Assert.That(stage.DataInput, Is.Null);
        Assert.That(config.GetInputStagesForStage(stage.StageId), Is.Empty);
        VerifyStage(stage, 5, "GenerateRecordsNoContextTaskStage", operation.TaskType.TaskType);
        VerifyDataOutput(stage, typeof(TextRecordWriter<int>));
        config.Validate();
        builder.TaskBuilder.DeleteAssembly();
    }

    [Test]
    public void TestInnerJoin()
    {
        JobBuilder builder = new JobBuilder(_fileSystemClient, _jetClient);

        var outer = builder.Read(_inputPath, typeof(RecordFileReader<double>));
        var inner = builder.Read(_inputPath2, typeof(RecordFileReader<int>));
        var joined = builder.InnerJoin(outer, inner, typeof(FakeInnerJoinRecordReader), typeof(FakeJoinComparer<double>), typeof(FakeJoinComparer<int>));
        builder.Write(joined, _outputPath, typeof(TextRecordWriter<>));

        JobConfiguration config = builder.CreateJob();

        VerifyAssemblyNames(config.AssemblyFileNames);

        Assert.That(config.Stages.Count, Is.EqualTo(3));

        VerifyDataInput(config, config.Stages[0], typeof(RecordFileReader<double>));
        VerifyStage(config.Stages[0], 3, "OuterReadStage", typeof(EmptyTask<double>));
        VerifyChannel(config.Stages[0], config.Stages[2], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<double>));
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[0], JumboSettings.FileChannel.Stage.SpillSortComparerType, typeof(FakeJoinComparer<double>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[0], PartitionerConstants.EqualityComparerSetting, typeof(FakeJoinComparer<double>).AssemblyQualifiedName);

        VerifyDataInput(config, config.Stages[1], typeof(RecordFileReader<int>), _inputPath2);
        VerifyStage(config.Stages[1], 3, "InnerReadStage", typeof(EmptyTask<int>));
        VerifyChannel(config.Stages[1], config.Stages[2], ChannelType.File, multiInputRecordReaderType: typeof(MergeRecordReader<int>));
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.SortSpill.ToString());
        VerifyStageSetting(config.Stages[1], JumboSettings.FileChannel.Stage.SpillSortComparerType, typeof(FakeJoinComparer<int>).AssemblyQualifiedName);
        VerifyStageSetting(config.Stages[1], PartitionerConstants.EqualityComparerSetting, typeof(FakeJoinComparer<int>).AssemblyQualifiedName);

        VerifyStage(config.Stages[2], 2, "JoinStage", typeof(EmptyTask<Utf8String>), typeof(FakeInnerJoinRecordReader));
        VerifyDataOutput(config.Stages[2], typeof(TextRecordWriter<Utf8String>));
    }

    private static void VerifyStage(StageConfiguration stage, int taskCount, string stageId, Type taskType, Type stageMultiInputRecordReader = null)
    {
        Assert.That(stage.StageId, Is.EqualTo(stageId));
        Assert.That(stage.TaskCount, Is.EqualTo(taskCount));
        Assert.That(stage.TaskType.GetReferencedType(), Is.EqualTo(taskType));
        if (stageMultiInputRecordReader != null)
        {
            Assert.That(stage.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(stageMultiInputRecordReader));
        }
        else
        {
            Assert.That(stage.MultiInputRecordReaderType.TypeName, Is.Null);
        }
    }

    private static void VerifyDataInput(JobConfiguration job, StageConfiguration stage, Type recordReaderType, string inputPath = _inputPath)
    {
        Assert.That(stage.DataInput, Is.Not.Null);
        Assert.That(stage.Parent, Is.Null);
        Assert.That(job.GetInputStagesForStage(stage.StageId), Is.Empty);
        Assert.That(stage.DataInput.TaskInputs.Count, Is.EqualTo(stage.TaskCount));
        Assert.That(stage.DataInput, Is.InstanceOf(typeof(FileDataInput)));
        Assert.That(stage.GetSetting(FileDataInput.RecordReaderTypeSettingKey, null), Is.EqualTo(recordReaderType.AssemblyQualifiedName));
        for (int x = 0; x < stage.TaskCount; ++x)
        {
            FileTaskInput input = (FileTaskInput)stage.DataInput.TaskInputs[x];
            Assert.That(input.Offset, Is.EqualTo(x * _blockSize));
            Assert.That(input.Path, Is.EqualTo(inputPath));
        }
    }

    private void VerifyDataOutput(StageConfiguration stage, Type recordWriterType, int blockSize = 0, int replicationFactor = 0)
    {
        Assert.That(stage.ChildStage, Is.Null);
        Assert.That(stage.OutputChannel, Is.Null);
        Assert.That(stage.DataOutput, Is.Not.Null);
        Assert.That(stage.HasDataOutput, Is.True);
        Type outputType = typeof(FileDataOutput);
        Assert.That(stage.DataOutput, Is.InstanceOf(outputType));
        Assert.That(stage.DataOutputType.GetReferencedType(), Is.EqualTo(outputType));
        Assert.That(stage.DataOutputType.TypeName, Is.EqualTo(outputType.AssemblyQualifiedName));
        Assert.That(stage.GetSetting(FileDataOutput.RecordWriterTypeSettingKey, null), Is.EqualTo(recordWriterType.AssemblyQualifiedName));
        Assert.That(stage.GetSetting(FileDataOutput.OutputPathFormatSettingKey, null), Is.EqualTo(_fileSystemClient.Path.Combine(_outputPath, stage.StageId + "-{0:00000}")));
        Assert.That(stage.GetSetting(FileDataOutput.BlockSizeSettingKey, 0), Is.EqualTo(blockSize));
        Assert.That(stage.GetSetting(FileDataOutput.ReplicationFactorSettingKey, 0), Is.EqualTo(replicationFactor));
    }

    private static void VerifyChannel(StageConfiguration sender, StageConfiguration receiver, ChannelType channelType, Type partitionerType = null, Type multiInputRecordReaderType = null, int partitionsPerTask = 1, PartitionAssignmentMethod assigmentMethod = PartitionAssignmentMethod.Linear)
    {
        TaskTypeInfo info = new TaskTypeInfo(sender.TaskType.GetReferencedType());
        if (partitionerType == null)
        {
            partitionerType = typeof(HashPartitioner<>).MakeGenericType(info.OutputRecordType);
        }

        if (multiInputRecordReaderType == null)
        {
            multiInputRecordReaderType = typeof(MultiRecordReader<>).MakeGenericType(info.OutputRecordType);
        }

        Assert.That(sender.DataOutput, Is.Null);
        Assert.That(sender.DataOutputType.TypeName, Is.Null);
        Assert.That(sender.HasDataOutput, Is.False);
        Assert.That(receiver.DataInput, Is.Null);
        if (channelType == ChannelType.Pipeline)
        {
            Assert.That(sender.OutputChannel, Is.Null);
            Assert.That(sender.ChildStage, Is.EqualTo(receiver));
            Assert.That(receiver.Parent, Is.EqualTo(sender));
            Assert.That(sender.ChildStagePartitionerType.GetReferencedType(), Is.EqualTo(partitionerType));
        }
        else
        {
            Assert.That(sender.OutputChannel, Is.Not.Null);
            Assert.That(sender.ChildStage, Is.Null);
            Assert.That(receiver.Parent, Is.Null);
            Assert.That(sender.OutputChannel.ChannelType, Is.EqualTo(channelType));
            Assert.That(sender.OutputChannel.OutputStage, Is.EqualTo(receiver.StageId));
            Assert.That(sender.OutputChannel.PartitionerType.GetReferencedType(), Is.EqualTo(partitionerType));
            Assert.That(sender.OutputChannel.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(multiInputRecordReaderType));
            Assert.That(sender.OutputChannel.PartitionsPerTask, Is.EqualTo(partitionsPerTask));
            Assert.That(sender.OutputChannel.PartitionAssignmentMethod, Is.EqualTo(assigmentMethod));
        }
    }

    private static void VerifyStageSetting(StageConfiguration stage, string settingName, string value)
    {
        Assert.That(stage.GetSetting(settingName, null), Is.EqualTo(value));
    }

    private static void VerifyAssemblyNames(IEnumerable<string> names)
    {
        HashSet<string> seen = new HashSet<string>();
        bool hasGenerated = false;
        foreach (string name in names)
        {
            if (name.StartsWith("Ookii.Jumbo.Jet.Generated."))
            {
                Assert.That(hasGenerated, Is.False, "More than one generated assembly.");
                hasGenerated = true;
            }
            else
            {
                Assert.That(seen.Contains(name), Is.False, $"Assembly name {name} duplicate");
                seen.Add(name);
                Assert.That(_allowedAssemblyNames.Contains(name), Is.True, $"Assembly name {name} not allowed");
            }
        }
    }
}
