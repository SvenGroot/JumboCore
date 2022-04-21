// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DeleteBlocksHeartbeatResponseTests
    {
        [Test]
        public void TestConstructor()
        {
            Guid blockID = Guid.NewGuid();
            Guid fsID = Guid.NewGuid();
            List<Guid> blocks = new List<Guid>() { blockID };
            DeleteBlocksHeartbeatResponse target = new DeleteBlocksHeartbeatResponse(fsID, blocks);
            Assert.AreEqual(fsID, target.FileSystemId);
            Assert.AreEqual(DataServerHeartbeatCommand.DeleteBlocks, target.Command);
            Assert.AreEqual(1, target.Blocks.Count());
            foreach (var id in target.Blocks)
                Assert.AreEqual(blockID, id);
        }
    }
}
