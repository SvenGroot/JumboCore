// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class ServerAdressTests
    {
        [Test]
        public void TestConstructorHostAndPort()
        {
            ServerAddress target = new ServerAddress("foo", 5000);
            Assert.AreEqual("foo", target.HostName);
            Assert.AreEqual(5000, target.Port);
        }

        [Test]
        public void TestEquals()
        {
            ServerAddress target1 = new ServerAddress("foo", 5000);
            ServerAddress target2 = new ServerAddress("foo", 5000);
            Assert.AreEqual(target1, target2);
            target2 = new ServerAddress("foo", 5001);
            Assert.AreNotEqual(target1, target2);
            target2 = new ServerAddress("bar", 5000);
            Assert.AreNotEqual(target1, target2);
        }

        [Test]
        public void TestGetHashCode()
        {
            ServerAddress target1 = new ServerAddress("foo", 5000);
            ServerAddress target2 = new ServerAddress("foo", 5000);
            Assert.AreEqual(target1.GetHashCode(), target2.GetHashCode());
            target2 = new ServerAddress("foo", 5001);
            Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
            target2 = new ServerAddress("bar", 5000);
            Assert.AreNotEqual(target1.GetHashCode(), target2.GetHashCode());
        }
    }
}
