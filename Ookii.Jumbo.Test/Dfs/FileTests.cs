// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using NameServerApplication;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class FileTests
    {
        private const int _blockSize = 16 * 1024 * 1024;
        private const int _replicationFactor = 1;

        [Test]
        public void TestConstructor()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize, _replicationFactor, IO.RecordStreamOptions.DoNotCrossBoundary);
            Assert.IsNotNull(target.Blocks);
            Assert.AreEqual(0, target.Blocks.Count);
            Assert.IsFalse(target.IsOpenForWriting);
            Assert.AreEqual(0, target.Size);
            Assert.AreEqual(_blockSize, target.BlockSize);
            Assert.AreEqual(_replicationFactor, target.ReplicationFactor);
            Assert.AreEqual(IO.RecordStreamOptions.DoNotCrossBoundary, target.RecordOptions);
        }

        [Test]
        public void TestIsOpenForWriting()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize, _replicationFactor, IO.RecordStreamOptions.None);
            bool expected = true;
            target.IsOpenForWriting = expected;
            Assert.AreEqual(expected, target.IsOpenForWriting);
        }

        [Test]
        public void TestSize()
        {
            DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize, _replicationFactor, IO.RecordStreamOptions.None);
            long expected = 0x1234567891234;
            target.Size = expected;
            Assert.AreEqual(expected, target.Size);
        }

        [Test]
        public void TestToJumboFile()
        {
            DfsDirectory parent = new DfsDirectory(null, string.Empty, DateTime.Now);
            DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, 10 * Packet.PacketSize, 3, IO.RecordStreamOptions.DoNotCrossBoundary) { Size = 1000 };
            target.Blocks.Add(Guid.NewGuid());
            JumboFile clone = target.ToJumboFile();
            Assert.AreNotSame(target, clone);
            Assert.AreEqual(target.Name, clone.Name);
            Assert.AreEqual(target.DateCreated, clone.DateCreated);
            Assert.AreEqual(target.FullPath, clone.FullPath);
            Assert.AreEqual(target.Size, clone.Size);
            Assert.AreEqual(target.BlockSize, clone.BlockSize);
            Assert.AreEqual(target.RecordOptions, clone.RecordOptions);
            Assert.AreEqual(target.ReplicationFactor, clone.ReplicationFactor);
            CollectionAssert.AreEqual(target.Blocks, clone.Blocks);
        }

    }
}
