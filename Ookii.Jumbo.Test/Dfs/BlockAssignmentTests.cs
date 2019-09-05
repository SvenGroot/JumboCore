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
    public class BlockAssignmentTests
    {
        [Test]
        public void TestConstructor()
        {
            Guid blockId = Guid.NewGuid();
            List<ServerAddress> servers = new List<ServerAddress>(new[] { new ServerAddress("foo", 1000), new ServerAddress("bar", 1001) });
            BlockAssignment target = new BlockAssignment(blockId, servers);
            Assert.AreEqual(blockId, target.BlockId);
            Assert.IsTrue(Utilities.CompareList(servers, target.DataServers));
        }
    }
}
