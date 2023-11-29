// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class NewBlockHeartbeatDataTests
{
    [Test]
    public void TestConstructor()
    {
        NewBlockHeartbeatData target = new NewBlockHeartbeatData();
        Assert.That(target.BlockId, Is.EqualTo(Guid.Empty));
        Assert.That(target.Size, Is.EqualTo(0));
    }

    [Test]
    public void TestBlockID()
    {
        NewBlockHeartbeatData target = new NewBlockHeartbeatData();
        Guid expected = Guid.NewGuid();
        target.BlockId = expected;
        Assert.That(target.BlockId, Is.EqualTo(expected));
    }

    [Test]
    public void TestSize()
    {
        NewBlockHeartbeatData target = new NewBlockHeartbeatData();
        int expected = 100;
        target.Size = expected;
        Assert.That(target.Size, Is.EqualTo(expected));
    }
}
