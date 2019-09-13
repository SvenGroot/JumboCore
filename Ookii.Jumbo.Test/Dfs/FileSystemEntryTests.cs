// Copyright (c) Sven Groot (Ookii.org)
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
    public class FileSystemEntryTests
    {
        private class FileSystemEntryDerived : DfsFileSystemEntry
        {
            public FileSystemEntryDerived(DfsDirectory parent, string name, DateTime dateCreated)
                : base(parent, name, dateCreated)
            {
            }

            protected override void LoadFromFileSystemImage(System.IO.BinaryReader reader, Action<long> notifyFileSizeCallback)
            {
            }

            public override Jumbo.Dfs.FileSystem.JumboFileSystemEntry ToJumboFileSystemEntry(bool includeChildren = true)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void TestConstructorNoParent()
        {
            DateTime date = DateTime.UtcNow;
            FileSystemEntryDerived target = new FileSystemEntryDerived(null, "Test", date);
            Assert.AreEqual(date, target.DateCreated);
            Assert.AreEqual("/", target.FullPath); // No parent means this always returns /
            Assert.AreEqual("Test", target.Name);
        }

        [Test]
        public void TestConstructorWithParent()
        {
            DateTime date = DateTime.UtcNow;
            string name = "test";
            DfsDirectory parent = new DfsDirectory(null, string.Empty, DateTime.Now);
            FileSystemEntryDerived target = new FileSystemEntryDerived(parent, name, date);
            Assert.AreEqual(date, target.DateCreated);
            Assert.AreEqual(name, target.Name);
            Assert.AreEqual("/test", target.FullPath);
        }

        [Test]
        public void TestName()
        {
            FileSystemEntryDerived target = new FileSystemEntryDerived(null, "foo", DateTime.UtcNow);
            string expected = "newName";
            target.Name = expected;
            Assert.AreEqual(expected, target.Name);
        }
    }
}
