// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    [Category("ClusterTest")]
    public class DfsClientTests
    {
        private TestDfsCluster _cluster;

        [OneTimeSetUp]
        public void Setup()
        {
            _cluster = new TestDfsCluster(1, 1);
            DfsClient client = _cluster.Client;
            client.WaitForSafeModeOff(Timeout.Infinite);
        }

        [OneTimeTearDown]
        public void Teardown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestCreateNameServerClient()
        {
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            INameServerClientProtocol client = DfsClient.CreateNameServerClient(config);
            Assert.IsNotNull(client);
            // Just checking if we can communicate, the value doesn't really matter all that much.
            Assert.AreEqual((int)config.NameServer.BlockSize, client.BlockSize);
        }

        [Test]
        public void TestCreateNameServerHeartbeatClient()
        {
            DfsConfiguration config = TestDfsCluster.CreateClientConfig();
            INameServerHeartbeatProtocol client = DfsClient.CreateNameServerHeartbeatClient(config);
            Assert.IsNotNull(client);
            // Just checking if we can communicate, the value doesn't really matter all that much.
            Assert.IsNotNull(client.Heartbeat(new ServerAddress("localhost", 9001), null));
        }

        [Test]
        public void TestUploadStream()
        {
            const int size = 1000000;
            FileSystemClient target = _cluster.Client;
            using (System.IO.MemoryStream stream = new System.IO.MemoryStream())
            {
                Utilities.GenerateData(stream, size);
                stream.Position = 0;
                target.UploadStream(stream, "/uploadstream");
            }
            JumboFile file = target.GetFileInfo("/uploadstream");
            Assert.IsNotNull(file);
            Assert.AreEqual(size, file.Size);
            Assert.IsFalse(file.IsOpenForWriting);
        }

        [Test]
        public void UploadFile()
        {
            string tempFile = System.IO.Path.GetTempFileName();
            try
            {
                const int size = 1000000;
                Utilities.GenerateFile(tempFile, size);
                FileSystemClient target = _cluster.Client;
                target.UploadFile(tempFile, "/uploadfile");
                JumboFile file = target.GetFileInfo("/uploadfile");
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.IsFalse(file.IsOpenForWriting);
            }
            finally
            {
                if (System.IO.File.Exists(tempFile))
                    System.IO.File.Delete(tempFile);
            }
        }

        [Test]
        public void UploadFileToDirectory()
        {
            string tempFile = System.IO.Path.GetTempFileName();
            try
            {
                const int size = 1000000;
                Utilities.GenerateFile(tempFile, size);
                FileSystemClient target = _cluster.Client;
                target.CreateDirectory("/uploadfiledir");
                target.UploadFile(tempFile, "/uploadfiledir");
                string fileName = System.IO.Path.GetFileName(tempFile);
                JumboFile file = target.GetFileInfo("/uploadfiledir/" + fileName);
                Assert.IsNotNull(file);
                Assert.AreEqual(size, file.Size);
                Assert.IsFalse(file.IsOpenForWriting);
            }
            finally
            {
                if (System.IO.File.Exists(tempFile))
                    System.IO.File.Delete(tempFile);
            }
        }

        [Test]
        public void TestDownloadStream()
        {
            const int size = 1000000;
            FileSystemClient target = _cluster.Client;
            using (System.IO.MemoryStream stream = new System.IO.MemoryStream())
            {
                Utilities.GenerateData(stream, size);
                stream.Position = 0;
                target.UploadStream(stream, "/downloadstream");
                using (System.IO.MemoryStream stream2 = new System.IO.MemoryStream())
                {
                    target.DownloadStream("/downloadstream", stream2);
                    stream2.Position = 0;
                    stream.Position = 0;
                    Assert.IsTrue(Utilities.CompareStream(stream, stream2));
                }
            }
        }
    }
}
