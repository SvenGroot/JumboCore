// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DfsConfigurationTests
{
    [Test]
    public void TestConstructor()
    {
        DfsConfiguration config = new DfsConfiguration();
        Assert.That(config.NameServer, Is.Not.Null);
        Assert.That(config.DataServer, Is.Not.Null);
    }
}
