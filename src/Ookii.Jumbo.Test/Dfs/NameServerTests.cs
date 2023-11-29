// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
[Category("ClusterTest")]
public class NameServerTests
{
    private const int _blockSize = 32 * 1024 * 1024;
    private TestDfsCluster _cluster;
    private INameServerClientProtocol _nameServer;

    [OneTimeSetUp]
    public void Setup()
    {
        _cluster = new TestDfsCluster(2, 1, _blockSize);
        Utilities.TraceLineAndFlush("Starting cluster.");
        DfsConfiguration config = TestDfsCluster.CreateClientConfig();
        _nameServer = DfsClient.CreateNameServerClient(config);
        _cluster.Client.WaitForSafeModeOff(Timeout.Infinite);
        Utilities.TraceLineAndFlush("Cluster started.");
    }

    [OneTimeTearDown]
    public void Teardown()
    {
        Utilities.TraceLineAndFlush("Shutting down cluster.");
        _cluster.Shutdown();
        Utilities.TraceLineAndFlush("Cluster shut down.");
    }

    /// <summary>
    ///A test for CreateDirectory
    ///</summary>
    [Test]
    public void CreateDirectoryGetDirectoryInfoTest()
    {
        INameServerClientProtocol target = _nameServer;
        string path = "/createdirectory/foo/bar";
        target.CreateDirectory(path);
        Ookii.Jumbo.Dfs.FileSystem.JumboDirectory result = target.GetDirectoryInfo(path);
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Name, Is.EqualTo("bar"));
        Assert.That(result.Children.Length, Is.EqualTo(0));
        Assert.That((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1, Is.True);
        DateTime oldDate = result.DateCreated;
        path = "/createdirectory/foo/bar/test";
        target.CreateDirectory(path);
        result = target.GetDirectoryInfo(path);
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Name, Is.EqualTo("test"));
        Assert.That(result.Children.Length, Is.EqualTo(0));
        Assert.That((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1, Is.True);
        // Recreating an old directory should return information about the existing one.
        path = "/createdirectory/foo/bar";
        target.CreateDirectory(path);
        result = target.GetDirectoryInfo(path);
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Name, Is.EqualTo("bar"));
        Assert.That(result.Children.Length, Is.EqualTo(1));
        Assert.That(result.DateCreated, Is.EqualTo(oldDate));
    }

    [Test]
    public void CreateDirectoryPathNullTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentNullException>(() => target.CreateDirectory(null));
    }

    [Test]
    public void CreateDirectoryNotRootedTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.CreateDirectory("test/foo"));
    }

    [Test]
    public void CreateDirectoryEmptyComponentTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.CreateDirectory("/createdirectory/test//"));
        Assert.That(target.GetDirectoryInfo("/createdirectory/test"), Is.Null);
    }

    [Test]
    public void CreateDirectoryPathContainsFileTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateDirectory("/createdirectory");
        target.CreateFile("/createdirectory/test", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/createdirectory/test");

        Assert.Throws<ArgumentException>(() => target.CreateDirectory("/createdirectory/test/foo"));
    }

    [Test]
    public void GetDirectoryInfoPathNullTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentNullException>(() => target.GetDirectoryInfo(null));
    }

    [Test]
    public void GetDirectoryInfoEmptyComponentTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.GetDirectoryInfo("/test//"));
    }

    [Test]
    public void GetDirectoryInfoNotRootedTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.GetDirectoryInfo("test/foo"));
    }

    [Test]
    public void GetDirectoryInfoPathContainsFileTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateFile("/getdirectoryinfotestfile", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/getdirectoryinfotestfile");
        Assert.Throws<ArgumentException>(() => target.GetDirectoryInfo("/getdirectoryinfotestfile/foo"));
    }

    [Test]
    public void CreateFileTest()
    {
        CreateFileTest("file1", 0, 0);
    }

    [Test]
    public void TestCreateFileCustomBlockSize()
    {
        CreateFileTest("file2", _blockSize * 2, 0);
    }

    [Test]
    public void TestCreateFileCustomReplicationFactor()
    {
        CreateFileTest("file3", 0, 2);
    }

    [Test]
    public void CreateFilePathNullTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentNullException>(() => target.CreateFile(null, 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileNameEmptyTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.CreateFile("/test/", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileDirectoryNotRootedTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.CreateFile("test/foo/test", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileDirectoryEmptyComponentTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.CreateFile("/test//test", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileDirectoryNotFoundTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<System.IO.DirectoryNotFoundException>(() => target.CreateFile("/test/test", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileExistingFileTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateFile("/existingfile", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/existingfile");
        Assert.Throws<ArgumentException>(() => target.CreateFile("/existingfile", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFileExistingDirectoryTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateDirectory("/existingdirectory");
        Assert.Throws<ArgumentException>(() => target.CreateFile("/existingdirectory", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void CreateFilePathContainsFileTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateFile("/test", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/test");
        Assert.Throws<ArgumentException>(() => target.CreateFile("/test/foo", 0, 0, true, IO.RecordStreamOptions.None));
    }

    [Test]
    public void GetFileInfoFileDoesntExistTest()
    {
        // Most of GetFileInfo is tested by CreateFile, so we just test whether it returns null for files that
        // don't exist.
        INameServerClientProtocol target = _nameServer;

        Assert.That(target.GetFileInfo("/asdf"), Is.Null);
        target.CreateDirectory("/getfiledirectory");
        Assert.That(target.GetFileInfo("/getfiledirectory"), Is.Null);
    }

    [Test]
    public void GetFileInfoPathNullTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentNullException>(() => target.GetFileInfo(null));
    }

    [Test]
    public void GetFileInfoNameEmptyTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.GetFileInfo("/test/"));
    }

    [Test]
    public void GetFileInfoDirectoryNotRootedTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.GetFileInfo("test"));
    }

    [Test]
    public void GetFileInfoDirectoryEmptyComponentTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<ArgumentException>(() => target.GetFileInfo("/test//test"));
    }

    [Test]
    public void GetFileInfoDirectoryNotFoundTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<DirectoryNotFoundException>(() => target.GetFileInfo("/directorythatdoesntexist/test"));
    }

    [Test]
    public void GetFileInfoPathContainsFileTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateFile("/getfileinfofile", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/getfileinfofile");
        Assert.Throws<ArgumentException>(() => target.GetFileInfo("/getfileinfofile/foo"));
    }

    [Test]
    public void GetFileSystemEntryInfoTest()
    {
        INameServerClientProtocol target = _nameServer;
        string directoryPath = "/getfilesystementryinfodir";
        string filePath = DfsPath.Combine(directoryPath, "somefile");
        target.CreateDirectory(directoryPath);
        target.CreateFile(filePath, 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile(filePath);

        JumboFileSystemEntry entry = target.GetFileSystemEntryInfo(directoryPath);
        Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = entry as Ookii.Jumbo.Dfs.FileSystem.JumboDirectory;
        Assert.That(dir, Is.Not.Null);
        Assert.That(dir.FullPath, Is.EqualTo(directoryPath));
        Assert.That(dir.Children.Length, Is.EqualTo(1));

        entry = target.GetFileSystemEntryInfo(filePath);
        Ookii.Jumbo.Dfs.FileSystem.JumboFile file = entry as Ookii.Jumbo.Dfs.FileSystem.JumboFile;
        Assert.That(file, Is.Not.Null);
        Assert.That(file.FullPath, Is.EqualTo(filePath));

        Assert.That(target.GetFileSystemEntryInfo("/directorythatdoesntexist"), Is.Null);
    }

    [Test]
    public void GetFileSystemEntryInfoDirectoryNotFoundTest()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.Throws<DirectoryNotFoundException>(() => target.GetFileSystemEntryInfo("/directorythatdoesntexist/test"));
    }

    [Test]
    public void DeleteTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateDirectory("/test1");
        target.CreateFile("/test1/test2", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/test1/test2");
        target.CreateFile("/test1/test3", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/test1/test3");

        bool result = target.Delete("/test1/test2", false);
        Assert.That(result, Is.True);
        result = target.Delete("/test1/test2", false);
        Assert.That(result, Is.False);
        result = target.Delete("/test1", true);
        Assert.That(result, Is.True);
        Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = target.GetDirectoryInfo("/test1");
        Assert.That(dir, Is.Null);

        using (DfsOutputStream stream = new DfsOutputStream(target, "/deletetest"))
        {
            Utilities.GenerateData(stream, 1000);
        }

        JumboFile file = target.GetFileInfo("/deletetest");
        ServerAddress[] servers = target.GetDataServersForBlock(file.Blocks[0]);
        Assert.That(servers.Length, Is.EqualTo(1));
        Assert.That(target.GetDataServerBlocks(servers[0]), Has.Member(file.Blocks[0]));

        result = target.Delete("/deletetest", false);
        Assert.That(result, Is.True);
        Assert.That(target.GetDataServerBlocks(servers[0]), Has.No.Member(file.Blocks[0]));
    }

    [Test]
    public void AppendBlockGetDataServersForBlockTest()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateDirectory("/appendblock");
        string path = "/appendblock/file";
        BlockAssignment block = target.CreateFile(path, 0, 0, true, IO.RecordStreamOptions.None);

        bool hasException = false;
        try
        {
            // This must fail because the file still has a pending block.
            target.AppendBlock(path, true);
        }
        catch (InvalidOperationException)
        {
            hasException = true;
        }
        Assert.That(hasException, Is.True);

        Packet p = new Packet();
        long sequenceNumber = 0;
        using (BlockSender sender = new BlockSender(block))
        {
            for (int sizeRemaining = target.BlockSize; sizeRemaining > 0; sizeRemaining -= Packet.PacketSize)
            {
                p.CopyFrom(Utilities.GenerateData(Packet.PacketSize), Packet.PacketSize, sequenceNumber++, sizeRemaining - Packet.PacketSize == 0);
                sender.SendPacket(p);
            }
            sender.WaitForAcknowledgements();
        }

        BlockAssignment block2 = target.AppendBlock(path, true);
        Assert.That(block2.BlockId, Is.Not.EqualTo(block.BlockId));
        Assert.That(block2.DataServers.Length, Is.EqualTo(1));
        Assert.That(block2.DataServers[0].HostName, Is.EqualTo(Dns.GetHostName()));
        Assert.That(block2.DataServers[0].Port == 10001 || block2.DataServers[0].Port == 10002, Is.True);

        sequenceNumber = 0;
        using (BlockSender sender = new BlockSender(block2))
        {
            p.CopyFrom(Utilities.GenerateData(10000), 10000, sequenceNumber++, true);
            sender.SendPacket(p);
            sender.WaitForAcknowledgements();
        }

        target.CloseFile(path);
        Ookii.Jumbo.Dfs.FileSystem.JumboFile file = target.GetFileInfo(path);
        Assert.That(file.Blocks.Length, Is.EqualTo(2));
        Assert.That(file.Blocks[0], Is.EqualTo(block.BlockId));
        Assert.That(file.Blocks[1], Is.EqualTo(block2.BlockId));
        Assert.That(file.Size, Is.EqualTo(target.BlockSize + 10000));

        ServerAddress[] servers = target.GetDataServersForBlock(block2.BlockId);
        Assert.That(Utilities.CompareList(block2.DataServers, servers), Is.True);
    }

    [Test]
    public void AppendBlockMultipleWritersTest()
    {
        INameServerClientProtocol target = _nameServer;
        // Because the blocks could get different data servers at random, we do it a couple of times to reduce the
        // chances fo passing this test by accident.
        for (int x = 0; x < 20; ++x)
        {
            target.CreateDirectory("/appendblockmultiplewriters");
            string path1 = "/appendblockmultiplewriters/file1";
            string path2 = "/appendblockmultiplewriters/file2";
            BlockAssignment block1 = target.CreateFile(path1, 0, 0, true, IO.RecordStreamOptions.None);
            BlockAssignment block2 = target.CreateFile(path2, 0, 0, true, IO.RecordStreamOptions.None);

            Assert.That(block2.BlockId, Is.Not.EqualTo(block1.BlockId));
            // Because the name server load balances based on pending blocks, and there is exactly one pending block in the system,
            // these should never be equal.
            Assert.That(block2.DataServers[0], Is.Not.EqualTo(block1.DataServers[0]));

            Packet p = new Packet();
            long sequenceNumber = 0;
            using (BlockSender sender = new BlockSender(block1))
            {
                p.CopyFrom(Utilities.GenerateData(10000), 10000, sequenceNumber++, true);
                sender.SendPacket(p);
                sender.WaitForAcknowledgements();
            }

            sequenceNumber = 0;
            using (BlockSender sender = new BlockSender(block2))
            {
                p.CopyFrom(Utilities.GenerateData(10000), 10000, sequenceNumber++, true);
                sender.SendPacket(p);
                sender.WaitForAcknowledgements();
            }

            target.CloseFile(path1);
            target.CloseFile(path2);
            target.Delete("/appendblockmultiplewriters", true);
        }
    }

    [Test]
    public void TestBlockSize()
    {
        INameServerClientProtocol target = _nameServer;
        Assert.That(target.BlockSize, Is.EqualTo(_blockSize));
    }

    [Test]
    public void TestCloseFilePendingBlock()
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateFile("/closefilependingblock", 0, 0, true, IO.RecordStreamOptions.None);
        target.CloseFile("/closefilependingblock");
        Ookii.Jumbo.Dfs.FileSystem.JumboFile file = target.GetFileInfo("/closefilependingblock");
        Assert.That(file.Blocks.Length, Is.EqualTo(0));
        Assert.That(target.GetMetrics().PendingBlockCount, Is.EqualTo(0));
    }

    [Test]
    public void TestGetMetrics()
    {
        INameServerClientProtocol target = _nameServer;
        DfsMetrics metrics = _nameServer.GetMetrics();
        Assert.That(metrics.DataServers.Count, Is.EqualTo(2));
        //Assert.AreEqual(new ServerAddress(Dns.GetHostName(), 10001), metrics.DataServers[0].Address);
        Assert.That(metrics.PendingBlockCount, Is.EqualTo(0));
        Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
        int initialBlockCount = metrics.TotalBlockCount;
        long initialSize = metrics.TotalSize;

        const int size = 10000000;
        using (DfsOutputStream output = new DfsOutputStream(target, "/metricstest"))
        {
            Utilities.GenerateData(output, size);
            metrics = _nameServer.GetMetrics();
            Assert.That(metrics.TotalSize, Is.EqualTo(initialSize)); // Block not committed: size isn't counted yet
            Assert.That(metrics.TotalBlockCount, Is.EqualTo(initialBlockCount));
            Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
            Assert.That(metrics.PendingBlockCount, Is.EqualTo(1));
        }
        metrics = _nameServer.GetMetrics();
        Assert.That(metrics.TotalSize, Is.EqualTo(initialSize + size)); // Block not committed: size isn't counted yet
        Assert.That(metrics.TotalBlockCount, Is.EqualTo(initialBlockCount + 1));
        Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
        Assert.That(metrics.PendingBlockCount, Is.EqualTo(0));
        target.Delete("/metricstest", false);
        metrics = _nameServer.GetMetrics();
        Assert.That(metrics.TotalSize, Is.EqualTo(initialSize));
        Assert.That(metrics.TotalBlockCount, Is.EqualTo(initialBlockCount));
        Assert.That(metrics.UnderReplicatedBlockCount, Is.EqualTo(0));
        Assert.That(metrics.PendingBlockCount, Is.EqualTo(0));
    }

    [Test]
    public void TestMove()
    {
        _nameServer.CreateDirectory("/move/dir1");
        _nameServer.CreateDirectory("/move/dir2");
        _nameServer.CreateFile("/move/dir1/file1", 0, 0, true, IO.RecordStreamOptions.None);
        _nameServer.CloseFile("/move/dir1/file1");
        // Test move to different file name in same directory
        _nameServer.Move("/move/dir1/file1", "/move/dir1/file2");
        Assert.That(_nameServer.GetFileInfo("/move/dir1/file1"), Is.Null);
        Ookii.Jumbo.Dfs.FileSystem.JumboFile file = _nameServer.GetFileInfo("/move/dir1/file2");
        Assert.That(file, Is.Not.Null);
        Assert.That(file.Name, Is.EqualTo("file2"));
        Assert.That(file.FullPath, Is.EqualTo("/move/dir1/file2"));
        // Test move to different directory without specifying file name.
        _nameServer.Move("/move/dir1/file2", "/move/dir2");
        Assert.That(_nameServer.GetFileInfo("/move/dir1/file2"), Is.Null);
        file = _nameServer.GetFileInfo("/move/dir2/file2");
        Assert.That(file, Is.Not.Null);
        Assert.That(file.Name, Is.EqualTo("file2"));
        Assert.That(file.FullPath, Is.EqualTo("/move/dir2/file2"));
        // Test move to different directory while specifying file name.
        _nameServer.Move("/move/dir2/file2", "/move/dir1/file3");
        Assert.That(_nameServer.GetFileInfo("/move/dir2/file2"), Is.Null);
        file = _nameServer.GetFileInfo("/move/dir1/file3");
        Assert.That(file, Is.Not.Null);
        Assert.That(file.Name, Is.EqualTo("file3"));
        Assert.That(file.FullPath, Is.EqualTo("/move/dir1/file3"));
        // Test move entire directory
        _nameServer.Move("/move/dir1", "/move/dir2");
        Assert.That(_nameServer.GetDirectoryInfo("/move/dir1"), Is.Null);
        Ookii.Jumbo.Dfs.FileSystem.JumboDirectory dir = _nameServer.GetDirectoryInfo("/move/dir2/dir1");
        Assert.That(dir, Is.Not.Null);
        Assert.That(dir.Name, Is.EqualTo("dir1"));
        Assert.That(dir.FullPath, Is.EqualTo("/move/dir2/dir1"));
        Assert.That(dir.Children.Length, Is.EqualTo(1));
        file = _nameServer.GetFileInfo("/move/dir2/dir1/file3");
        Assert.That(file, Is.Not.Null);
        Assert.That(file.Name, Is.EqualTo("file3"));
        Assert.That(file.FullPath, Is.EqualTo("/move/dir2/dir1/file3"));
    }

    [Test]
    public void TestDeletePendingFile()
    {
        const string fileName = "/deletependingfile";
        using (DfsOutputStream stream = new DfsOutputStream(_nameServer, fileName))
        {
            Utilities.GenerateData(stream, 1000);
            _nameServer.Delete(fileName, false);
            bool hasException = false;
            try
            {
                stream.Close();
            }
            catch (InvalidOperationException)
            {
                hasException = true;
            }
            Assert.That(hasException, Is.True);
        }
    }

    private void CreateFileTest(string fileName, int blockSize, int replicationFactor)
    {
        INameServerClientProtocol target = _nameServer;
        target.CreateDirectory("/createfile");
        string path = DfsPath.Combine("/createfile", fileName);
        BlockAssignment block = target.CreateFile(path, blockSize, replicationFactor, true, IO.RecordStreamOptions.None);
        Assert.That(block.DataServers.Length, Is.EqualTo(replicationFactor == 0 ? 1 : replicationFactor));
        Assert.That(block.DataServers[0].HostName, Is.EqualTo(Dns.GetHostName()));
        //Assert.AreEqual(10001, block.DataServers[0].Port);
        Ookii.Jumbo.Dfs.FileSystem.JumboFile result = target.GetFileInfo(path);
        Assert.That((result.DateCreated - DateTime.UtcNow).TotalSeconds < 1, Is.True);
        Assert.That(result.Name, Is.EqualTo(fileName));
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Blocks.Length, Is.EqualTo(0));
        Assert.That(result.Size, Is.EqualTo(0));
        Assert.That(result.BlockSize, Is.EqualTo(blockSize == 0 ? _nameServer.BlockSize : blockSize));
        Assert.That(result.ReplicationFactor, Is.EqualTo(replicationFactor == 0 ? 1 : replicationFactor));
        Assert.That(result.IsOpenForWriting, Is.True);

        using (BlockSender sender = new BlockSender(block))
        {
            sender.SendPacket(new Packet(Utilities.GenerateData(10000), 10000, 1, true));
            sender.WaitForAcknowledgements();
        }

        result = target.GetFileInfo(path);
        Assert.That(result.Name, Is.EqualTo(fileName));
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Blocks.Length, Is.EqualTo(1));
        Assert.That(result.Blocks[0], Is.EqualTo(block.BlockId));
        Assert.That(result.Size, Is.EqualTo(10000));
        Assert.That(result.BlockSize, Is.EqualTo(blockSize == 0 ? _nameServer.BlockSize : blockSize));
        Assert.That(result.IsOpenForWriting, Is.True);

        target.CloseFile(path);

        result = target.GetFileInfo(path);
        Assert.That(result.Name, Is.EqualTo(fileName));
        Assert.That(result.FullPath, Is.EqualTo(path));
        Assert.That(result.Blocks.Length, Is.EqualTo(1));
        Assert.That(result.Blocks[0], Is.EqualTo(block.BlockId));
        Assert.That(result.Size, Is.EqualTo(10000));
        Assert.That(result.BlockSize, Is.EqualTo(blockSize == 0 ? _nameServer.BlockSize : blockSize));
        Assert.That(result.IsOpenForWriting, Is.False);
    }
}
