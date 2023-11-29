// Copyright (c) Sven Groot (Ookii.org)
using System;
using NameServerApplication;
using NUnit.Framework;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class DirectoryTests
{
    private const int _blockSize = 16 * 1024 * 1024;

    [Test]
    public void TestConstructor()
    {
        DfsDirectory target = new DfsDirectory(null, "", DateTime.UtcNow);
        Assert.That(target.Children, Is.Not.Null);
        Assert.That(target.Children.Count, Is.EqualTo(0));
    }

    [Test]
    public void TestChildren()
    {
        DfsDirectory target = CreateDirectoryStructure();
        Assert.That(target.Children.Count, Is.EqualTo(1));
        Assert.That(target.Children[0].Name, Is.EqualTo("child1"));
        Assert.That(target.Children[0].FullPath, Is.EqualTo("/child1"));
        Assert.That(((DfsDirectory)target.Children[0]).Children.Count, Is.EqualTo(2));
        Assert.That(((DfsDirectory)target.Children[0]).Children[0].Name, Is.EqualTo("child2"));
        Assert.That(((DfsDirectory)target.Children[0]).Children[0].FullPath, Is.EqualTo("/child1/child2"));
        Assert.That(((DfsDirectory)target.Children[0]).Children[1].Name, Is.EqualTo("child3"));
        Assert.That(((DfsDirectory)target.Children[0]).Children[1].FullPath, Is.EqualTo("/child1/child3"));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children.Count, Is.EqualTo(1));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].Name, Is.EqualTo("child4"));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].FullPath, Is.EqualTo("/child1/child2/child4"));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[0]).Children[0].GetType(), Is.EqualTo(typeof(DfsFile)));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children.Count, Is.EqualTo(1));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].Name, Is.EqualTo("child5"));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].FullPath, Is.EqualTo("/child1/child3/child5"));
        Assert.That(((DfsDirectory)((DfsDirectory)target.Children[0]).Children[1]).Children[0].GetType(), Is.EqualTo(typeof(DfsDirectory)));
    }

    [Test]
    public void TestToJumboDirectory()
    {
        DfsDirectory target = CreateDirectoryStructure();
        DfsDirectory child1 = (DfsDirectory)target.Children[0];
        JumboDirectory clone = child1.ToJumboDirectory();
        Assert.That(clone.Name, Is.EqualTo("child1"));
        Assert.That(clone.FullPath, Is.EqualTo("/child1"));
        //Assert.IsNull(clone.Parent);
        Assert.That(clone.Children.Length, Is.EqualTo(2));
        Assert.That(clone.Children[0].Name, Is.EqualTo("child2"));
        Assert.That(clone.Children[1].Name, Is.EqualTo("child3"));
        Assert.That(clone.Children[0].FullPath, Is.EqualTo("/child1/child2"));
        Assert.That(clone.Children[1].FullPath, Is.EqualTo("/child1/child3"));
        // Check the level below the children wasn't cloned.
        Assert.That(((JumboDirectory)clone.Children[0]).Children.Length, Is.EqualTo(0));
        Assert.That(((JumboDirectory)clone.Children[1]).Children.Length, Is.EqualTo(0));

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
