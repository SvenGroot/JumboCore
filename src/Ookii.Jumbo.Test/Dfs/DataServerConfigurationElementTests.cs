// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DataServerConfigurationElementTests
{
    [Test]
    public void TestConstructor()
    {
        DataServerConfigurationElement elt = new DataServerConfigurationElement();
        Assert.That(elt.Port, Is.EqualTo(9001));
        Assert.That(elt.BlockStorageDirectory, Is.EqualTo(string.Empty));
        Assert.That(elt.ListenIPv4AndIPv6, Is.True);
    }

    [Test]
    public void TestPort()
    {
        DataServerConfigurationElement target = new DataServerConfigurationElement();
        int expected = 10000;
        target.Port = expected;
        Assert.That(target.Port, Is.EqualTo(expected));
    }

    [Test]
    public void TestBlockStoragePath()
    {
        DataServerConfigurationElement target = new DataServerConfigurationElement();
        string expected = "foo";
        target.BlockStorageDirectory = expected;
        Assert.That(target.BlockStorageDirectory, Is.EqualTo(expected));
    }

    [Test]
    public void TestListenIPv4AndIPv6()
    {
        DataServerConfigurationElement target = new DataServerConfigurationElement();
        bool expected = false;
        target.ListenIPv4AndIPv6 = expected;
        Assert.That(target.ListenIPv4AndIPv6, Is.EqualTo(expected));
    }
}
