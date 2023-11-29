// Copyright (c) Sven Groot (Ookii.org)
using System;
using NameServerApplication;
using NUnit.Framework;

namespace Ookii.Jumbo.Test.Dfs;

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
        Assert.That(target.DateCreated, Is.EqualTo(date));
        Assert.That(target.FullPath, Is.EqualTo("/")); // No parent means this always returns /
        Assert.That(target.Name, Is.EqualTo("Test"));
    }

    [Test]
    public void TestConstructorWithParent()
    {
        DateTime date = DateTime.UtcNow;
        string name = "test";
        DfsDirectory parent = new DfsDirectory(null, string.Empty, DateTime.Now);
        FileSystemEntryDerived target = new FileSystemEntryDerived(parent, name, date);
        Assert.That(target.DateCreated, Is.EqualTo(date));
        Assert.That(target.Name, Is.EqualTo(name));
        Assert.That(target.FullPath, Is.EqualTo("/test"));
    }

    [Test]
    public void TestName()
    {
        FileSystemEntryDerived target = new FileSystemEntryDerived(null, "foo", DateTime.UtcNow);
        string expected = "newName";
        target.Name = expected;
        Assert.That(target.Name, Is.EqualTo(expected));
    }
}
