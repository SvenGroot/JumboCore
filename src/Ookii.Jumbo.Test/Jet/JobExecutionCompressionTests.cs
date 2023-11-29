// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Test.Jet;

[TestFixture]
[Category("JetClusterTests")]
public class JobExecutionCompressionTests : JobExecutionTestsBase
{
    [Test]
    public void TestMemorySort()
    {
        FileSystemClient client = Cluster.FileSystemClient;
        JobConfiguration config = CreateMemorySortJob(client, null, ChannelType.File, 1);
        config.AddSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferSize, "3MB");
        config.AddTypedSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, true);
        RunJob(client, config);
        VerifySortOutput(client, config);
    }

    [Test]
    public void TestSpillSort()
    {
        RunSpillSortJob(false, true);
    }

    [Test]
    public void TestSpillSortFileChannelDownload()
    {
        RunSpillSortJob(true, true);
    }

    [Test]
    public void TestSpillSortFileChannelDownloadNoMemoryStorage()
    {
        RunSpillSortJob(false, false);
    }

    private JobStatus RunSpillSortJob(bool forceFileDownload, bool useMemoryStorage)
    {
        FileSystemClient client = Cluster.FileSystemClient;
        JobConfiguration config = CreateSpillSortJob(client, null, 1, forceFileDownload);
        config.AddSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferSize, "3MB");
        config.AddTypedSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, true);
        if (!useMemoryStorage)
        {
            config.AddTypedSetting(JumboSettings.FileChannel.StageOrJob.MemoryStorageSize, 0L);
        }

        JobStatus status = RunJob(client, config);
        VerifySortOutput(client, config);
        return status;
    }

    protected override TestJetCluster CreateCluster()
    {
        return new TestJetCluster(16777216, true, 2, CompressionType.GZip);
    }
}
