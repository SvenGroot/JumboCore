// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Dfs
{
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
            Assert.IsInstanceOf<LocalFileSystemClient>(client);
            LocalFileSystemClient localClient = (LocalFileSystemClient)client;
            Assert.IsNull(localClient.RootPath);

            Uri rootUri = new Uri(new Uri("file://"), _testPath);
            config.FileSystem.Url = rootUri;
            client = FileSystemClient.Create(config);
            Assert.IsInstanceOf<LocalFileSystemClient>(client);
            localClient = (LocalFileSystemClient)client;
            Assert.AreEqual(_testPath, localClient.RootPath);
        }

        [Test]
        public void TestConfiguration()
        {
            LocalFileSystemClient target = new LocalFileSystemClient();
            Assert.AreEqual(new Uri("file:///"), target.Configuration.FileSystem.Url);
        }

        [Test]
        public void TestDefaultBlockSize()
        {
            LocalFileSystemClient target = new LocalFileSystemClient();
            Assert.IsNull(target.DefaultBlockSize);
        }

        [Test]
        public void TestGetDirectoryInfo()
        {
            LocalFileSystemClient target = new LocalFileSystemClient();
            JumboDirectory actual = target.GetDirectoryInfo(_testPath);
            VerifyDirectory(actual);
            actual = target.GetDirectoryInfo(target.Path.Combine(_testPath, "NotExist"));
            Assert.IsNull(actual);
        }

        [Test]
        public void TestGetDirectoryInfoRootPath()
        {
            LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
            JumboDirectory actual = target.GetDirectoryInfo("/");
            VerifyDirectory(actual, _testPath);
            actual = target.GetDirectoryInfo("/NotExist");
            Assert.IsNull(actual, _testPath);
            actual = target.GetDirectoryInfo("/Foo");
            Assert.IsNotNull(actual);
        }

        [Test]
        public void TestGetFileInfo()
        {
            LocalFileSystemClient target = new LocalFileSystemClient();
            JumboFile actual = target.GetFileInfo(target.Path.Combine(_testPath, "test1.dat"));
            VerifyFile(actual);
            actual = target.GetFileInfo(target.Path.Combine(_testPath, "NotExist"));
            Assert.IsNull(actual);
        }

        [Test]
        public void TestGetFileInfoRootPath()
        {
            LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
            JumboFile actual = target.GetFileInfo("/test1.dat");
            VerifyFile(actual, _testPath);
            actual = target.GetFileInfo("/NotExist");
            Assert.IsNull(actual, _testPath);
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
            Assert.IsNull(actual);
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
            Assert.IsNull(actual);
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
            Assert.IsTrue(file.Exists);
            Assert.AreEqual(250, file.Length);
            target.Delete(path, false);
            Assert.IsFalse(File.Exists(path));
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
            Assert.IsTrue(file.Exists);
            Assert.AreEqual(250, file.Length);
            target.Delete(path, false);
            Assert.IsFalse(File.Exists(Path.Combine(_testPath, "create.dat")));
        }

        [Test]
        public void TestCreateAndDeleteDirectory()
        {
            LocalFileSystemClient target = new LocalFileSystemClient();
            string path = target.Path.Combine(_testPath, "Subdir");
            target.CreateDirectory(path);
            Assert.IsTrue(Directory.Exists(path));
            target.Delete(path, true);
            Assert.IsFalse(Directory.Exists(path));
        }

        [Test]
        public void TestCreateAndDeleteDirectoryRootPath()
        {
            LocalFileSystemClient target = new LocalFileSystemClient(_testPath);
            string path = "/Subdir";
            target.CreateDirectory(path);
            Assert.IsTrue(Directory.Exists(Path.Combine(_testPath, "Subdir")));
            target.Delete(path, true);
            Assert.IsFalse(Directory.Exists(Path.Combine(_testPath, "Subdir")));
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
            CollectionAssert.AreEqual(expected, actual);
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
            CollectionAssert.AreEqual(expected, actual);
        }

        private static void VerifyDirectory(JumboDirectory directory, string rootPath = null, bool includeChildren = true)
        {
            Assert.IsNotNull(directory);
            DirectoryInfo info = new DirectoryInfo(AdjustPath(rootPath, directory.FullPath));
            //Assert.AreEqual(info.FullName, directory.FullPath);
            Assert.AreEqual(directory.FullPath.Length == 1 ? "" : info.Name, directory.Name);
            Assert.AreEqual(info.CreationTimeUtc, directory.DateCreated);
            if (includeChildren)
            {
                FileSystemInfo[] children = info.GetFileSystemInfos();
                Assert.AreEqual(children.Length, directory.Children.Length);
                foreach (var child in children)
                {
                    VerifyEntry(directory.GetChild(child.Name), rootPath, false);
                }
            }
            else
                CollectionAssert.IsEmpty(directory.Children);
        }

        private static void VerifyFile(JumboFile file, string rootPath = null)
        {
            Assert.IsNotNull(file);
            FileInfo info = new FileInfo(AdjustPath(rootPath, file.FullPath));
            //Assert.AreEqual(info.FullName, file.FullPath);
            Assert.AreEqual(info.Name, file.Name);
            Assert.AreEqual(info.CreationTimeUtc, file.DateCreated);
            Assert.AreEqual(info.Length, file.Size);
            Assert.AreEqual(info.Length, file.BlockSize);
            Assert.AreEqual(1, file.ReplicationFactor);
            Assert.AreEqual(RecordStreamOptions.None, file.RecordOptions);
            Assert.IsFalse(file.IsOpenForWriting);
            Assert.AreEqual(1, file.Blocks.Length);
            Assert.AreEqual(Guid.Empty, file.Blocks[0]);
        }

        private static void VerifyEntry(JumboFileSystemEntry entry, string rootPath = null, bool includeChildren = true)
        {
            Assert.IsNotNull(entry);
            JumboFile file = entry as JumboFile;
            if (file != null)
                VerifyFile(file, rootPath);
            else
                VerifyDirectory((JumboDirectory)entry, rootPath, includeChildren);
        }

        private static string AdjustPath(string rootPath, string path)
        {
            if (rootPath == null)
                return path;
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
}
