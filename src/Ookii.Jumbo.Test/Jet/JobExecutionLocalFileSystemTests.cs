// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTests")]
    public class JobExecutionLocalFileSystemTests : JobExecutionTestsBase
    {
        protected override TestJetCluster CreateCluster()
        {
            return new TestJetCluster(16777216, true, 2, CompressionType.None, true);
        }

        [Test]
        public void TestWordCount()
        {
            RunWordCountJob(null, TaskKind.Pull, Jumbo.Jet.Channels.ChannelType.File, false);
        }

        [Test]
        public void TestWordCountMaxSplitSize()
        {
            RunWordCountJob(null, TaskKind.Pull, Jumbo.Jet.Channels.ChannelType.File, false, 16777216);
        }
    }
}
