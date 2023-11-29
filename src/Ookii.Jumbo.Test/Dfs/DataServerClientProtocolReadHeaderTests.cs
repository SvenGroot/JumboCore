// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DataServerClientProtocolReadHeaderTests
{
    [Test]
    public void TestConstructor()
    {
        DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
        Assert.That(target.Command, Is.EqualTo(DataServerCommand.ReadBlock));
        Assert.That(target.BlockId, Is.EqualTo(Guid.Empty));
        Assert.That(target.Offset, Is.EqualTo(0));
        Assert.That(target.Size, Is.EqualTo(0));
    }

    [Test]
    public void TestOffset()
    {
        DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
        int expected = 10000;
        target.Offset = expected;
        Assert.That(target.Offset, Is.EqualTo(expected));
    }

    [Test]
    public void TestSize()
    {
        DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
        int expected = 10000;
        target.Size = expected;
        Assert.That(target.Size, Is.EqualTo(expected));
    }
}
