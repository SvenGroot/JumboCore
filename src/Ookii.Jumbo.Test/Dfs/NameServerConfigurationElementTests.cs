// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class NameServerConfigurationElementTests
{
    [Test]
    public void TestConstructor()
    {
        NameServerConfigurationElement target = new NameServerConfigurationElement();
        Assert.That((int)target.BlockSize, Is.EqualTo(67108864));
        Assert.That(target.ReplicationFactor, Is.EqualTo(1));
        Assert.That(target.ListenIPv4AndIPv6, Is.True);
        Assert.That(target.ImageDirectory, Is.EqualTo(string.Empty));
    }

    [Test]
    public void TestBlockSize()
    {
        NameServerConfigurationElement target = new NameServerConfigurationElement();
        BinarySize expected = (BinarySize)(20 * Packet.PacketSize);
        target.BlockSize = expected;
        Assert.That(target.BlockSize, Is.EqualTo(expected));
    }

    [Test]
    public void TestReplicationFactor()
    {
        NameServerConfigurationElement target = new NameServerConfigurationElement();
        int expected = 3;
        target.ReplicationFactor = expected;
        Assert.That(target.ReplicationFactor, Is.EqualTo(expected));
    }

    [Test]
    public void TestListenIPv4AndIPv6()
    {
        NameServerConfigurationElement target = new NameServerConfigurationElement();
        bool expected = false;
        target.ListenIPv4AndIPv6 = expected;
        Assert.That(target.ListenIPv4AndIPv6, Is.EqualTo(expected));
    }

    [Test]
    public void TestEditLogDirectory()
    {
        NameServerConfigurationElement target = new NameServerConfigurationElement();
        string expected = "c:\\log";
        target.ImageDirectory = expected;
        Assert.That(target.ImageDirectory, Is.EqualTo(expected));
    }
}
