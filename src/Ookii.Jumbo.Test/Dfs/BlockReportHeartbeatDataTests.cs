// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class BlockReportHeartbeatDataTests
    {
        [Test]
        public void TestConstructor()
        {
            Guid[] expected = new Guid[] { new Guid() };
            BlockReportHeartbeatData target = new BlockReportHeartbeatData(expected);
            Assert.IsTrue(Utilities.CompareList(expected, target.Blocks));
        }
    }
}
