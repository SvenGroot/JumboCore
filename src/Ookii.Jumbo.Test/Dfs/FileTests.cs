// Copyright (c) Sven Groot (Ookii.org)
using System;
using NameServerApplication;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

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
        Assert.That(target.Blocks, Is.Not.Null);
        Assert.That(target.Blocks.Count, Is.EqualTo(0));
        Assert.That(target.IsOpenForWriting, Is.False);
        Assert.That(target.Size, Is.EqualTo(0));
        Assert.That(target.BlockSize, Is.EqualTo(_blockSize));
        Assert.That(target.ReplicationFactor, Is.EqualTo(_replicationFactor));
        Assert.That(target.RecordOptions, Is.EqualTo(IO.RecordStreamOptions.DoNotCrossBoundary));
    }

    [Test]
    public void TestIsOpenForWriting()
    {
        DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
        DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize, _replicationFactor, IO.RecordStreamOptions.None);
        bool expected = true;
        target.IsOpenForWriting = expected;
        Assert.That(target.IsOpenForWriting, Is.EqualTo(expected));
    }

    [Test]
    public void TestSize()
    {
        DfsDirectory parent = new DfsDirectory(null, "", DateTime.UtcNow);
        DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, _blockSize, _replicationFactor, IO.RecordStreamOptions.None);
        long expected = 0x1234567891234;
        target.Size = expected;
        Assert.That(target.Size, Is.EqualTo(expected));
    }

    [Test]
    public void TestToJumboFile()
    {
        DfsDirectory parent = new DfsDirectory(null, string.Empty, DateTime.Now);
        DfsFile target = new DfsFile(parent, "test", DateTime.UtcNow, 10 * Packet.PacketSize, 3, IO.RecordStreamOptions.DoNotCrossBoundary) { Size = 1000 };
        target.Blocks.Add(Guid.NewGuid());
        JumboFile clone = target.ToJumboFile();
        Assert.That(clone.Name, Is.EqualTo(target.Name));
        Assert.That(clone.DateCreated, Is.EqualTo(target.DateCreated));
        Assert.That(clone.FullPath, Is.EqualTo(target.FullPath));
        Assert.That(clone.Size, Is.EqualTo(target.Size));
        Assert.That(clone.BlockSize, Is.EqualTo(target.BlockSize));
        Assert.That(clone.RecordOptions, Is.EqualTo(target.RecordOptions));
        Assert.That(clone.ReplicationFactor, Is.EqualTo(target.ReplicationFactor));
        Assert.That(clone.Blocks, Is.EqualTo(target.Blocks).AsCollection);
    }

}
