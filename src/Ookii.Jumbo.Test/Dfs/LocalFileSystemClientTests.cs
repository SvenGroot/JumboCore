// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class LocalFileSystemClientTests
{
    private string _testPath;

    [OneTimeSetUp]
    public void SetUp()
    {
        _testPath = Path.Combine(Utilities.TestOutputPath, "LocalFileSystemClientTests");
        Directory.CreateDirectory(_testPath);
        Utilities.GenerateFile(Path.Combine(_testPath, "test1.dat"), 1000);
        Utilities.GenerateFile(Path.Combine(_testPath, "test2.dat"), 500);
        string subDirectory = Path.Combine(_testPath, "Foo");
        Directory.CreateDirectory(subDirectory);
        Utilities.GenerateFile(Path.Combine(subDirectory, "test3.dat"), 200);
    }

    [OneTimeTearDown]
    public void TearDown()
    {
        Directory.Delete(_testPath, true);
    }

    [Test]
    public void TestCreateFromConfiguration()
    {
        DfsConfiguration config = new DfsConfiguration();
        config.FileSystem.Url = new Uri("file://");
        FileSystemClient client = FileSystemClient.Create(config);
        Assert.That(client, Is.InstanceOf<LocalFileSystemClient>());
        LocalFileSystemClient localClient = (LocalFileSystemClient)client;
        Assert.That(localClient.RootPath, Is.Null);

        Uri rootUri = new Uri(new Uri("file://"), _testPath);
        config.FileSystem.Url = rootUri;
        client = FileSystemClient.Create(config);
        Assert.That(client, Is.InstanceOf<LocalFileSystemClient>());
        localClient = (LocalFileSystemClient)client;
        Assert.That(localClient.RootPath, Is.EqualTo(_testPath));
    }

    [Test]
    public void TestConfiguration()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        Assert.That(target.Configuration.FileSystem.Url, Is.EqualTo(new Uri("file:///")));
    }

    [Test]
    public void TestDefaultBlockSize()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        Assert.That(target.DefaultBlockSize, Is.Null);
    }

    [Test]
    public void TestGetDirectoryInfo()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        JumboDirectory actual = target.GetDirectoryInfo(_testPath);
        VerifyDirectory(actual);
        actual = target.GetDirectoryInfo(target.Path.Combine(_testPath, "NotExist"));
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TestGetDirectoryInfoRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        JumboDirectory actual = target.GetDirectoryInfo("/");
        VerifyDirectory(actual, _testPath);
        actual = target.GetDirectoryInfo("/NotExist");
        Assert.That(actual, Is.Null, _testPath);
        actual = target.GetDirectoryInfo("/Foo");
        Assert.That(actual, Is.Not.Null);
    }

    [Test]
    public void TestGetFileInfo()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        JumboFile actual = target.GetFileInfo(target.Path.Combine(_testPath, "test1.dat"));
        VerifyFile(actual);
        actual = target.GetFileInfo(target.Path.Combine(_testPath, "NotExist"));
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TestGetFileInfoRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        JumboFile actual = target.GetFileInfo("/test1.dat");
        VerifyFile(actual, _testPath);
        actual = target.GetFileInfo("/NotExist");
        Assert.That(actual, Is.Null, _testPath);
    }

    [Test]
    public void TestGetFileSystemEntryInfo()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        JumboFileSystemEntry actual = target.GetFileSystemEntryInfo(_testPath);
        VerifyDirectory((JumboDirectory)actual);
        actual = target.GetFileSystemEntryInfo(target.Path.Combine(_testPath, "test1.dat"));
        VerifyFile((JumboFile)actual);
        actual = target.GetFileSystemEntryInfo(target.Path.Combine(_testPath, "NotExist"));
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TestGetFileSystemEntryInfoRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        JumboFileSystemEntry actual = target.GetFileSystemEntryInfo("/");
        VerifyDirectory((JumboDirectory)actual, _testPath);
        actual = target.GetFileSystemEntryInfo("/test1.dat");
        VerifyFile((JumboFile)actual, _testPath);
        actual = target.GetFileSystemEntryInfo("/NotExist");
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void TestCreateAndDeleteFile()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        string path = target.Path.Combine(_testPath, "create.dat");
        using (Stream stream = target.CreateFile(path))
        {
            Utilities.GenerateData(stream, 250);
        }
        FileInfo file = new FileInfo(path);
        Assert.That(file.Exists, Is.True);
        Assert.That(file.Length, Is.EqualTo(250));
        target.Delete(path, false);
        Assert.That(File.Exists(path), Is.False);
    }

    [Test]
    public void TestCreateAndDeleteFileRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        string path = "/create.dat";
        using (Stream stream = target.CreateFile(path))
        {
            Utilities.GenerateData(stream, 250);
        }
        FileInfo file = new FileInfo(Path.Combine(_testPath, "create.dat"));
        Assert.That(file.Exists, Is.True);
        Assert.That(file.Length, Is.EqualTo(250));
        target.Delete(path, false);
        Assert.That(File.Exists(Path.Combine(_testPath, "create.dat")), Is.False);
    }

    [Test]
    public void TestCreateAndDeleteDirectory()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        string path = target.Path.Combine(_testPath, "Subdir");
        target.CreateDirectory(path);
        Assert.That(Directory.Exists(path), Is.True);
        target.Delete(path, true);
        Assert.That(Directory.Exists(path), Is.False);
    }

    [Test]
    public void TestCreateAndDeleteDirectoryRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        string path = "/Subdir";
        target.CreateDirectory(path);
        Assert.That(Directory.Exists(Path.Combine(_testPath, "Subdir")), Is.True);
        target.Delete(path, true);
        Assert.That(Directory.Exists(Path.Combine(_testPath, "Subdir")), Is.False);
    }

    [Test]
    public void TestOpenFile()
    {
        LocalFileSystemClient target = new LocalFileSystemClient();
        string path = target.Path.Combine(_testPath, "test1.dat");
        byte[] actual;
        using (Stream stream = target.OpenFile(path))
        {
            actual = new byte[stream.Length];
            stream.Read(actual, 0, actual.Length);
        }
        byte[] expected = File.ReadAllBytes(path);
        Assert.That(actual, Is.EqualTo(expected).AsCollection);
    }

    [Test]
    public void TestOpenFileRootPath()
    {
        LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
        string path = "/test1.dat";
        byte[] actual;
        using (Stream stream = target.OpenFile(path))
        {
            actual = new byte[stream.Length];
            stream.Read(actual, 0, actual.Length);
        }
        byte[] expected = File.ReadAllBytes(Path.Combine(_testPath, "test1.dat"));
        Assert.That(actual, Is.EqualTo(expected).AsCollection);
    }

    private static void VerifyDirectory(JumboDirectory directory, string rootPath = null, bool includeChildren = true)
    {
        Assert.That(directory, Is.Not.Null);
        DirectoryInfo info = new DirectoryInfo(AdjustPath(rootPath, directory.FullPath));
        //Assert.AreEqual(info.FullName, directory.FullPath);
        Assert.That(directory.Name, Is.EqualTo(directory.FullPath.Length == 1 ? "" : info.Name));
        Assert.That(directory.DateCreated, Is.EqualTo(info.CreationTimeUtc));
        if (includeChildren)
        {
            FileSystemInfo[] children = info.GetFileSystemInfos();
            Assert.That(directory.Children.Length, Is.EqualTo(children.Length));
            foreach (var child in children)
            {
                VerifyEntry(directory.GetChild(child.Name), rootPath, false);
            }
        }
        else
        {
            Assert.That(directory.Children, Is.Empty);
        }
    }

    private static void VerifyFile(JumboFile file, string rootPath = null)
    {
        Assert.That(file, Is.Not.Null);
        FileInfo info = new FileInfo(AdjustPath(rootPath, file.FullPath));
        //Assert.AreEqual(info.FullName, file.FullPath);
        Assert.That(file.Name, Is.EqualTo(info.Name));
        Assert.That(file.DateCreated, Is.EqualTo(info.CreationTimeUtc));
        Assert.That(file.Size, Is.EqualTo(info.Length));
        Assert.That(file.BlockSize, Is.EqualTo(info.Length));
        Assert.That(file.ReplicationFactor, Is.EqualTo(1));
        Assert.That(file.RecordOptions, Is.EqualTo(RecordStreamOptions.None));
        Assert.That(file.IsOpenForWriting, Is.False);
        Assert.That(file.Blocks.Length, Is.EqualTo(1));
        Assert.That(file.Blocks[0], Is.EqualTo(Guid.Empty));
    }

    private static void VerifyEntry(JumboFileSystemEntry entry, string rootPath = null, bool includeChildren = true)
    {
        Assert.That(entry, Is.Not.Null);
        JumboFile file = entry as JumboFile;
        if (file != null)
        {
            VerifyFile(file, rootPath);
        }
        else
        {
            VerifyDirectory((JumboDirectory)entry, rootPath, includeChildren);
        }
    }

    private static string AdjustPath(string rootPath, string path)
    {
        if (rootPath == null)
        {
            return path;
        }
        else
        {
            if (System.IO.Path.IsPathRooted(path))
            {
                int rootLength = System.IO.Path.GetPathRoot(path).Length;
                path = path.Substring(rootLength);
            }
            return System.IO.Path.Combine(rootPath, path);
        }
    }
}
