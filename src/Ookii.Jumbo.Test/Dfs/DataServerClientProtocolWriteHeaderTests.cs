// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DataServerClientProtocolWriteHeaderTests
{
    [Test]
    public void TestConstructor()
    {
        ServerAddress[] expected = new ServerAddress[] { new ServerAddress("localhost", 9000) };
        DataServerClientProtocolWriteHeader target = new DataServerClientProtocolWriteHeader(expected);
        Assert.That(target.Command, Is.EqualTo(DataServerCommand.WriteBlock));
        Assert.That(target.BlockId, Is.EqualTo(Guid.Empty));
        Assert.That(Utilities.CompareList(expected, target.DataServers), Is.True);
    }
}
