// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DataServerClientProtocolReadHeaderTests
    {
        [Test]
        public void TestConstructor()
        {
            DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
            Assert.AreEqual(DataServerCommand.ReadBlock, target.Command);
            Assert.AreEqual(Guid.Empty, target.BlockId);
            Assert.AreEqual(0, target.Offset);
            Assert.AreEqual(0, target.Size);
        }

        [Test]
        public void TestOffset()
        {
            DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
            int expected = 10000;
            target.Offset = expected;
            Assert.AreEqual(expected, target.Offset);
        }

        [Test]
        public void TestSize()
        {
            DataServerClientProtocolReadHeader target = new DataServerClientProtocolReadHeader();
            int expected = 10000;
            target.Size = expected;
            Assert.AreEqual(expected, target.Size);
        }
    }
}
