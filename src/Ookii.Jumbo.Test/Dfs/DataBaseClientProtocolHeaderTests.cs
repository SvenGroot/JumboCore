// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DataBaseClientProtocolHeaderTests
{
    private class Header : DataServerClientProtocolHeader
    {
        public Header(DataServerCommand command)
            : base(command)
        {
        }
    }

    [Test]
    public void TestConstructor()
    {
        DataServerClientProtocolHeader target = new Header(DataServerCommand.WriteBlock);
        Assert.That(target.Command, Is.EqualTo(DataServerCommand.WriteBlock));
        Assert.That(target.BlockId, Is.EqualTo(Guid.Empty));
        target = new Header(DataServerCommand.ReadBlock);
        Assert.That(target.Command, Is.EqualTo(DataServerCommand.ReadBlock));
        Assert.That(target.BlockId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void TestBlockID()
    {
        DataServerClientProtocolHeader target = new Header(DataServerCommand.ReadBlock);
        Guid expected = Guid.NewGuid();
        target.BlockId = expected;
        Assert.That(target.BlockId, Is.EqualTo(expected));
    }
}
