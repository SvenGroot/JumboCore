// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

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
        Assert.That(target.FileSystemId, Is.EqualTo(fsID));
        Assert.That(target.Command, Is.EqualTo(DataServerHeartbeatCommand.DeleteBlocks));
        Assert.That(target.Blocks.Count(), Is.EqualTo(1));
        foreach (var id in target.Blocks)
        {
            Assert.That(id, Is.EqualTo(blockID));
        }
    }
}
