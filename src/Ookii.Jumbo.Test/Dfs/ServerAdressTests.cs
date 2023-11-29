// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class ServerAdressTests
{
    [Test]
    public void TestConstructorHostAndPort()
    {
        ServerAddress target = new ServerAddress("foo", 5000);
        Assert.That(target.HostName, Is.EqualTo("foo"));
        Assert.That(target.Port, Is.EqualTo(5000));
    }

    [Test]
    public void TestEquals()
    {
        ServerAddress target1 = new ServerAddress("foo", 5000);
        ServerAddress target2 = new ServerAddress("foo", 5000);
        Assert.That(target2, Is.EqualTo(target1));
        target2 = new ServerAddress("foo", 5001);
        Assert.That(target2, Is.Not.EqualTo(target1));
        target2 = new ServerAddress("bar", 5000);
        Assert.That(target2, Is.Not.EqualTo(target1));
    }

    [Test]
    public void TestGetHashCode()
    {
        ServerAddress target1 = new ServerAddress("foo", 5000);
        ServerAddress target2 = new ServerAddress("foo", 5000);
        Assert.That(target2.GetHashCode(), Is.EqualTo(target1.GetHashCode()));
        target2 = new ServerAddress("foo", 5001);
        Assert.That(target2.GetHashCode(), Is.Not.EqualTo(target1.GetHashCode()));
        target2 = new ServerAddress("bar", 5000);
        Assert.That(target2.GetHashCode(), Is.Not.EqualTo(target1.GetHashCode()));
    }
}
