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
    public class NameServerConfigurationElementTests
    {
        [Test]
        public void TestConstructor()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            Assert.AreEqual(67108864, (int)target.BlockSize);
            Assert.AreEqual(1, target.ReplicationFactor);
            Assert.IsTrue(target.ListenIPv4AndIPv6);
            Assert.AreEqual(string.Empty, target.ImageDirectory);
        }

        [Test]
        public void TestBlockSize()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            BinarySize expected = 20 * Packet.PacketSize;
            target.BlockSize = expected;
            Assert.AreEqual(expected, target.BlockSize);
        }

        [Test]
        public void TestReplicationFactor()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            int expected = 3;
            target.ReplicationFactor = expected;
            Assert.AreEqual(expected, target.ReplicationFactor);
        }

        [Test]
        public void TestListenIPv4AndIPv6()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            bool expected = false;
            target.ListenIPv4AndIPv6 = expected;
            Assert.AreEqual(expected, target.ListenIPv4AndIPv6);
        }

        [Test]
        public void TestEditLogDirectory()
        {
            NameServerConfigurationElement target = new NameServerConfigurationElement();
            string expected = "c:\\log" ;
            target.ImageDirectory = expected;
            Assert.AreEqual(expected, target.ImageDirectory);
        }
    }
}
