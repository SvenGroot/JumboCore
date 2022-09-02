// Copyright (c) Sven Groot (Ookii.org)
using System;
using NameServerApplication;
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class DirectoryTests
    {
        private const int _blockSize = 16 * 1024 * 1024;

        [Test]
        public void TestConstructor()
        {
            DfsDirectory target = new DfsDirectory(null, "", DateTime.UtcNow);
            Assert.IsNotNull(target.Children);
            Assert.AreEqual(0, target.Children.Count);
        }

        [Test]
        public void TestChildren()
        {
            DfsDirectory target = CreateDirectoryStructure();
            Assert.AreEqual(1, target.Children.Count);
            Assert.AreEqual("child1", target.Children[0].Name);
            Assert.AreEqual("/child1", target.Children[0].FullPath);
            Assert.AreEqual(2, ((DfsDirectory)target.Children[0]).Children.Count);
            Assert.AreEqual("child2", ((DfsDirectory)target.Children[0]).Children[0].Name);
            Assert.AreEqual("/child1/child2", ((DfsDirectory)target.Children[0]).Children[0].FullPath);
            Assert.AreEqual("child3", ((DfsDirectory)target.Children[0]).Children[1].Name);
            Assert.AreEqual("/child1/child3", ((DfsDirectory)target.Children[0]).Children[1].FullPath);
            Assert.AreEqual(1, ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children.Count);
            Assert.AreEqual("child4", ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].Name);
            Assert.AreEqual("/child1/child2/child4", ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].FullPath);
            Assert.AreEqual(typeof(DfsFile), ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].GetType());
            Assert.AreEqual(1, ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children.Count);
            Assert.AreEqual("child5", ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].Name);
            Assert.AreEqual("/child1/child3/child5", ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].FullPath);
            Assert.AreEqual(typeof(DfsDirectory), ((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].GetType());
        }

        [Test]
        public void TestToJumboDirectory()
        {
            DfsDirectory target = CreateDirectoryStructure();
            DfsDirectory child1 = (DfsDirectory)target.Children[0];
            JumboDirectory clone = child1.ToJumboDirectory();
            Assert.AreNotSame(child1, clone);
            Assert.AreEqual("child1", clone.Name);
            Assert.AreEqual("/child1", clone.FullPath);
            //Assert.IsNull(clone.Parent);
            Assert.AreEqual(2, clone.Children.Count);
            Assert.AreNotSame(child1.Children, clone.Children);
            Assert.AreEqual("child2", clone.Children[0].Name);
            Assert.AreEqual("child3", clone.Children[1].Name);
            Assert.AreEqual("/child1/child2", clone.Children[0].FullPath);
            Assert.AreEqual("/child1/child3", clone.Children[1].FullPath);
            // Check the level below the children wasn't cloned.
            Assert.AreEqual(0, ((JumboDirectory)clone.Children[0]).Children.Count);
            Assert.AreEqual(0, ((JumboDirectory)clone.Children[1]).Children.Count);

        }

        private DfsDirectory CreateDirectoryStructure()
        {
            /* Create directory structure
             * /
             * /child1/
             * /child1/child2
             * /child1/child2/child4
             * /child1/child3
             * /child1/child3/child5
             */
            DfsDirectory root = new DfsDirectory(null, "", DateTime.UtcNow);
            DfsDirectory child1 = new DfsDirectory(root, "child1", DateTime.UtcNow);
            DfsDirectory child2 = new DfsDirectory(child1, "child2", DateTime.UtcNow);
            DfsDirectory child3 = new DfsDirectory(child1, "child3", DateTime.UtcNow);
            new DfsFile(child2, "child4", DateTime.UtcNow, _blockSize, 1, IO.RecordStreamOptions.None);
            new DfsDirectory(child3, "child5", DateTime.UtcNow);
            return root;
        }
    }
}
