// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Test.Jet;

[TestFixture]
public class JobConfigurationTests
{
    #region Nested types

    private sealed class FakeFileSystemClient : FileSystemClient
    {
        public FakeFileSystemClient(DfsConfiguration configuration)
            : base(configuration)
        {
        }

        public override IFileSystemPathUtility Path
        {
            get { return new DfsPathUtility(); }
        }

        public override int? DefaultBlockSize
        {
            get { return _blockSize; }
        }

        public override JumboDirectory GetDirectoryInfo(string path)
        {
            if (path == "/output")
            {
                return new JumboDirectory("/output", "output", DateTime.UtcNow, null);
            }

            return null;
        }

        public override JumboFile GetFileInfo(string path)
        {
            if (path.StartsWith("/test"))
            {
                return new JumboFile(path, Path.GetFileName(path), DateTime.UtcNow, 5 * _blockSize, _blockSize, 1, RecordStreamOptions.None, false, Enumerable.Repeat(Guid.Empty, 5));
            }

            return null;
        }

        public override JumboFileSystemEntry GetFileSystemEntryInfo(string path)
        {
            return GetFileInfo(path) ?? (JumboFileSystemEntry)GetDirectoryInfo(path);
        }

        public override void CreateDirectory(string path)
        {
            throw new NotImplementedException();
        }

        public override System.IO.Stream OpenFile(string path)
        {
            throw new NotImplementedException();
        }

        public override System.IO.Stream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            throw new NotImplementedException();
        }

        public override bool Delete(string path, bool recursive)
        {
            throw new NotImplementedException();
        }

        public override void Move(string source, string destination)
        {
            throw new NotImplementedException();
        }
    }

    #endregion

    private const int _blockSize = 16 * 1024 * 1024;
    private DfsConfiguration _fakeConfiguration = CreateFakeConfiguration();
    private FakeFileSystemClient _fileSystem;

    [OneTimeSetUp]
    public void SetUp()
    {
        _fileSystem = new FakeFileSystemClient(_fakeConfiguration);
    }

    [Test]
    public void TestConstructor()
    {
        JobConfiguration target = new JobConfiguration();
        Assert.That(target.AssemblyFileNames, Is.Not.Null);
        Assert.That(target.Stages, Is.Not.Null);
        Assert.That(target.AssemblyFileNames.Count, Is.EqualTo(0));
        Assert.That(target.Stages.Count, Is.EqualTo(0));
    }

    [Test]
    public void TestConstructorAssemblies()
    {
        JobConfiguration target = new JobConfiguration(typeof(Tasks.LineAdderTask).Assembly, typeof(JobConfigurationTests).Assembly);
        Assert.That(target.AssemblyFileNames, Is.Not.Null);
        Assert.That(target.Stages, Is.Not.Null);
        Assert.That(target.Stages.Count, Is.EqualTo(0));
        Assert.That(target.AssemblyFileNames.Count, Is.EqualTo(2));
        Assert.That(target.AssemblyFileNames[0], Is.EqualTo(System.IO.Path.GetFileName(typeof(Tasks.LineAdderTask).Assembly.Location)));
        Assert.That(target.AssemblyFileNames[1], Is.EqualTo(System.IO.Path.GetFileName(typeof(JobConfigurationTests).Assembly.Location)));
    }

    [Test]
    public void TestConstructorAssemblyFileNames()
    {
        JobConfiguration target = new JobConfiguration("foo.dll", "bar.dll");
        Assert.That(target.AssemblyFileNames, Is.Not.Null);
        Assert.That(target.Stages, Is.Not.Null);
        Assert.That(target.Stages.Count, Is.EqualTo(0));
        Assert.That(target.AssemblyFileNames.Count, Is.EqualTo(2));
        Assert.That(target.AssemblyFileNames[0], Is.EqualTo("foo.dll"));
        Assert.That(target.AssemblyFileNames[1], Is.EqualTo("bar.dll"));
    }

    [Test]
    public void TestAddInputStage()
    {
        JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
        JumboFile file = CreateFakeTestFile("test");
        const int splitsPerBlock = 2;

        StageConfiguration stage = target.AddDataInputStage("InputStage", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file, maxSplitSize: _blockSize / splitsPerBlock), typeof(Tasks.LineCounterTask));

        Assert.That(stage.DataInput, Is.Not.Null);
        Assert.That(stage.HasDataInput, Is.True);
        Assert.That(stage.TaskCount, Is.EqualTo(file.Blocks.Length * splitsPerBlock));
        Assert.That(stage.DataInput.TaskInputs.Count, Is.EqualTo(stage.TaskCount));
        Assert.That(target.Stages.Count, Is.EqualTo(1));
        Assert.That(target.Stages[0], Is.EqualTo(stage));
        Assert.That(stage.StageId, Is.EqualTo("InputStage"));
        Assert.That(stage.DataInput.TaskInputs.Count, Is.EqualTo(file.Blocks.Length * splitsPerBlock));
        Assert.That(stage.DataInput, Is.InstanceOf<FileDataInput>());
        Assert.That(stage.DataInputType.GetReferencedType(), Is.EqualTo(typeof(FileDataInput)));
        Assert.That(stage.DataInputType.TypeName, Is.EqualTo(typeof(FileDataInput).AssemblyQualifiedName));
        Assert.That(stage.GetSetting(FileDataInput.RecordReaderTypeSettingKey, null), Is.EqualTo(typeof(LineRecordReader).AssemblyQualifiedName));
        Assert.That(stage.GetSetting(FileDataInput.InputPathSettingKey, null), Is.EqualTo(file.FullPath));
        int x = 0;
        foreach (FileTaskInput input in stage.DataInput.TaskInputs)
        {
            Assert.That(input.Offset, Is.EqualTo(x++ * (_blockSize / splitsPerBlock)));
            Assert.That(input.Size, Is.EqualTo(_blockSize / splitsPerBlock));
        }
        Assert.That(stage.DataOutput, Is.Null);
        Assert.That(stage.DataOutputType.TypeName, Is.Null);
        Assert.That(stage.HasDataOutput, Is.False);
        Assert.That(stage.TaskType.TypeName, Is.EqualTo(typeof(Tasks.LineCounterTask).AssemblyQualifiedName));
        Assert.That(stage.TaskType.GetReferencedType(), Is.EqualTo(typeof(Tasks.LineCounterTask)));
        target.Validate();

    }

    [Test]
    public void TestAddStageWithoutDfsOutput()
    {
        TestAddStage(false);
    }

    [Test]
    public void TestAddStageWithDfsOutput()
    {
        TestAddStage(true);
    }

    [Test]
    public void TestGetStage()
    {
        JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
        JumboFile file = CreateFakeTestFile("test1");

        StageConfiguration expected = target.AddDataInputStage("InputStage", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file), typeof(Tasks.LineCounterTask));

        StageConfiguration stage = target.GetStage("InputStage");
        Assert.That(stage, Is.Not.Null);
        Assert.That(stage, Is.SameAs(expected));
        Assert.That(stage.StageId, Is.EqualTo("InputStage"));

        Assert.That(target.GetStage("StageNameThatDoesn'tExist"), Is.Null);
    }

    [Test]
    public void TestGetInputStagesForStage()
    {
        JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
        JumboFile file1 = CreateFakeTestFile("test1");
        JumboFile file2 = CreateFakeTestFile("test2");

        StageConfiguration inputStage1 = target.AddDataInputStage("InputStage1", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file1), typeof(Tasks.LineCounterTask));
        StageConfiguration inputStage2 = target.AddDataInputStage("InputStage2", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file2), typeof(Tasks.LineCounterTask));

        const int taskCount = 3;
        const string outputPath = "/output";
        var stage = target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>));
        stage.DataOutput = new FileDataOutput(_fakeConfiguration, typeof(TextRecordWriter<int>), outputPath);


        List<StageConfiguration> stages = target.GetInputStagesForStage("SecondStage").ToList();

        Assert.That(stages.Contains(inputStage1), Is.True);
        Assert.That(stages.Contains(inputStage2), Is.True);
        Assert.That(stages.Count, Is.EqualTo(2));
        Assert.That(target.GetInputStagesForStage("InputStage1").Count(), Is.EqualTo(0)); // exists but has no input channel.
        Assert.That(target.GetInputStagesForStage("BadName").Count(), Is.EqualTo(0));
    }

    [Test]
    public void TestAddStageMultiplePartitionsPerTask()
    {
        JobConfiguration target = new JobConfiguration();
        JumboFile file1 = CreateFakeTestFile("test1");

        StageConfiguration inputStage = target.AddDataInputStage("InputStage", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file1), typeof(SortTask<Utf8String>));

        const int taskCount = 3;
        const int partitionsPerTask = 5;

        StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(inputStage) { PartitionsPerTask = partitionsPerTask });
        stage.DataOutput = new FileDataOutput(_fakeConfiguration, typeof(TextRecordWriter<Utf8String>), "/output");

        ChannelConfiguration channel = inputStage.OutputChannel;
        Assert.That(channel.ChannelType, Is.EqualTo(ChannelType.File));
        Assert.That(channel.ForceFileDownload, Is.False);
        Assert.That(channel.PartitionerType.TypeName, Is.EqualTo(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName));
        Assert.That(channel.PartitionerType.GetReferencedType(), Is.EqualTo(typeof(HashPartitioner<Utf8String>)));
        Assert.That(channel.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(typeof(MultiRecordReader<Utf8String>)));
        Assert.That(channel.MultiInputRecordReaderType.TypeName, Is.EqualTo(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName));
        Assert.That(channel.OutputStage, Is.EqualTo(stage.StageId));
        Assert.That(channel.PartitionsPerTask, Is.EqualTo(partitionsPerTask));
        target.Validate();
    }

    [Test]
    public void TestAddStageMultiplePartitionsPerTaskInternalPartitioning()
    {
        JobConfiguration target = new JobConfiguration();
        JumboFile file1 = CreateFakeTestFile("test1");

        const int taskCount = 3;
        const int partitionsPerTask = 5;

        StageConfiguration inputStage = target.AddDataInputStage("InputStage", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file1), typeof(EmptyTask<Utf8String>));
        StageConfiguration sortStage = target.AddStage("SortStage", typeof(SortTask<Utf8String>), taskCount * partitionsPerTask, new InputStageInfo(inputStage) { ChannelType = ChannelType.Pipeline });

        StageConfiguration stage = target.AddStage("SecondStage", typeof(EmptyTask<Utf8String>), taskCount, new InputStageInfo(sortStage) { PartitionsPerTask = partitionsPerTask });
        stage.DataOutput = new FileDataOutput(_fakeConfiguration, typeof(TextRecordWriter<Utf8String>), "/output");

        ChannelConfiguration channel = sortStage.OutputChannel;
        Assert.That(channel.ChannelType, Is.EqualTo(ChannelType.File));
        Assert.That(channel.ForceFileDownload, Is.False);
        Assert.That(channel.PartitionerType.TypeName, Is.EqualTo(typeof(HashPartitioner<Utf8String>).AssemblyQualifiedName));
        Assert.That(channel.PartitionerType.GetReferencedType(), Is.EqualTo(typeof(HashPartitioner<Utf8String>)));
        Assert.That(channel.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(typeof(MultiRecordReader<Utf8String>)));
        Assert.That(channel.MultiInputRecordReaderType.TypeName, Is.EqualTo(typeof(MultiRecordReader<Utf8String>).AssemblyQualifiedName));
        Assert.That(channel.OutputStage, Is.EqualTo(stage.StageId));
        Assert.That(channel.PartitionsPerTask, Is.EqualTo(partitionsPerTask));
        target.Validate();
    }


    private void TestAddStage(bool useOutput)
    {
        JobConfiguration target = new JobConfiguration(typeof(Tasks.LineCounterTask).Assembly);
        JumboFile file1 = CreateFakeTestFile("test1");
        JumboFile file2 = CreateFakeTestFile("test2");

        StageConfiguration inputStage1 = target.AddDataInputStage("InputStage1", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file1), typeof(Tasks.LineCounterTask));
        StageConfiguration inputStage2 = target.AddDataInputStage("InputStage2", new FileDataInput(_fakeConfiguration, typeof(LineRecordReader), file2), typeof(Tasks.LineCounterTask));

        const int taskCount = 3;
        const string outputPath = "/output";
        StageConfiguration stage = target.AddStage("SecondStage", typeof(Tasks.LineAdderTask), taskCount, new[] { new InputStageInfo(inputStage1), new InputStageInfo(inputStage2) }, typeof(MultiRecordReader<int>));
        if (useOutput)
        {
            stage.DataOutput = new FileDataOutput(_fakeConfiguration, typeof(TextRecordWriter<int>), outputPath);
        }

        Assert.That(stage.TaskCount, Is.EqualTo(taskCount));
        Assert.That(target.Stages.Count, Is.EqualTo(3));
        Assert.That(target.Stages[2], Is.EqualTo(stage));

        Assert.That(stage.StageId, Is.EqualTo("SecondStage"));
        Assert.That(stage.DataInput, Is.Null);
        Assert.That(stage.DataInputType.TypeName, Is.Null);
        Assert.That(stage.HasDataInput, Is.False);
        if (useOutput)
        {
            Assert.That(stage.DataOutput, Is.Not.Null);
            Assert.That(stage.HasDataOutput, Is.True);
            Type outputType = typeof(FileDataOutput);
            Assert.That(stage.DataOutput, Is.InstanceOf(outputType));
            Assert.That(stage.DataOutputType.GetReferencedType(), Is.EqualTo(outputType));
            Assert.That(stage.DataOutputType.TypeName, Is.EqualTo(outputType.AssemblyQualifiedName));
            Assert.That(stage.GetSetting(FileDataOutput.RecordWriterTypeSettingKey, null), Is.EqualTo(typeof(TextRecordWriter<int>).AssemblyQualifiedName));
            Assert.That(stage.GetSetting(FileDataOutput.OutputPathFormatSettingKey, null), Is.EqualTo(DfsPath.Combine(outputPath, stage.StageId + "-{0:00000}")));
            Assert.That(stage.GetSetting(FileDataOutput.BlockSizeSettingKey, 0), Is.EqualTo(0));
            Assert.That(stage.GetSetting(FileDataOutput.ReplicationFactorSettingKey, 0), Is.EqualTo(0));
        }
        else
        {
            Assert.That(stage.DataOutput, Is.Null);
            Assert.That(stage.DataOutputType.TypeName, Is.Null);
            Assert.That(stage.HasDataOutput, Is.False);
        }

        Assert.That(stage.TaskType.TypeName, Is.EqualTo(typeof(Tasks.LineAdderTask).AssemblyQualifiedName));
        Assert.That(stage.TaskType.GetReferencedType(), Is.EqualTo(typeof(Tasks.LineAdderTask)));

        ChannelConfiguration channel = inputStage1.OutputChannel;
        Assert.That(channel.ChannelType, Is.EqualTo(ChannelType.File));
        Assert.That(channel.ForceFileDownload, Is.False);
        Assert.That(channel.PartitionerType.TypeName, Is.EqualTo(typeof(HashPartitioner<int>).AssemblyQualifiedName));
        Assert.That(channel.PartitionerType.GetReferencedType(), Is.EqualTo(typeof(HashPartitioner<int>)));
        Assert.That(channel.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(typeof(MultiRecordReader<int>)));
        Assert.That(channel.MultiInputRecordReaderType.TypeName, Is.EqualTo(typeof(MultiRecordReader<int>).AssemblyQualifiedName));
        Assert.That(channel.OutputStage, Is.EqualTo(stage.StageId));
        Assert.That(channel.PartitionsPerTask, Is.EqualTo(1));
        channel = inputStage2.OutputChannel;
        Assert.That(channel.ChannelType, Is.EqualTo(ChannelType.File));
        Assert.That(channel.ForceFileDownload, Is.False);
        Assert.That(channel.PartitionerType.TypeName, Is.EqualTo(typeof(HashPartitioner<int>).AssemblyQualifiedName));
        Assert.That(channel.PartitionerType.GetReferencedType(), Is.EqualTo(typeof(HashPartitioner<int>)));
        Assert.That(channel.MultiInputRecordReaderType.GetReferencedType(), Is.EqualTo(typeof(MultiRecordReader<int>)));
        Assert.That(channel.MultiInputRecordReaderType.TypeName, Is.EqualTo(typeof(MultiRecordReader<int>).AssemblyQualifiedName));
        Assert.That(channel.OutputStage, Is.EqualTo(stage.StageId));
        Assert.That(channel.PartitionsPerTask, Is.EqualTo(1));
        target.Validate();
    }

    private JumboFile CreateFakeTestFile(string name)
    {
        return _fileSystem.GetFileInfo("/" + name);
    }

    private static DfsConfiguration CreateFakeConfiguration()
    {
        FileSystemClient.RegisterFileSystem("fake", typeof(FakeFileSystemClient));
        DfsConfiguration config = new DfsConfiguration();
        config.FileSystem.Url = new Uri("fake://");
        return config;
    }
}
