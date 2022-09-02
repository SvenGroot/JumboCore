// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DataServerClientProtocolWriteHeaderTests
    {
        [Test]
        public void TestConstructor()
        {
            ServerAddress[] expected = new ServerAddress[] { new ServerAddress("localhost", 9000) };
            DataServerClientProtocolWriteHeader target = new DataServerClientProtocolWriteHeader(expected);
            Assert.AreEqual(DataServerCommand.WriteBlock, target.Command);
            Assert.AreEqual(Guid.Empty, target.BlockId);
            Assert.IsTrue(Utilities.CompareList(expected, target.DataServers));
        }
    }
}
