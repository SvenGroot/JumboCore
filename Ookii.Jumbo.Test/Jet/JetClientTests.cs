// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    [Category("JetClusterTest")]
    public class JetClientTests
    {
        private TestJetCluster _cluster;

        [OneTimeSetUp]
        public void Setup()
        {
            _cluster = new TestJetCluster(null, true, 4, CompressionType.None);
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _cluster.Shutdown();
        }

        [Test]
        public void TestCreateJobServerClient()
        {
            IJobServerClientProtocol client = JetClient.CreateJobServerClient(TestJetCluster.CreateClientConfig());
            // We're not checking the result, just seeing if we can communicate.
            Assert.IsNotNull(client.CreateJob());
        }

        [Test]
        public void TestCreateJobServerHeartbeatClient()
        {
            IJobServerHeartbeatProtocol client = JetClient.CreateJobServerHeartbeatClient(TestJetCluster.CreateClientConfig());
            client.Heartbeat(new ServerAddress("localhost", 15000), null);
        }

        [Test]
        public void TestCreateTaskServerUmbilicalClient()
        {
            ITaskServerUmbilicalProtocol client = JetClient.CreateTaskServerUmbilicalClient(TestJetCluster.TaskServerPort);
            Assert.Throws<ArgumentNullException>(() => client.ReportCompletion(Guid.Empty, null, null));
        }

        [Test]
        public void TestCreateTaskServerClient()
        {
            ITaskServerClientProtocol client = JetClient.CreateTaskServerClient(new ServerAddress("localhost", TestJetCluster.TaskServerPort));
            Assert.AreEqual(TaskAttemptStatus.NotStarted, client.GetTaskStatus(Guid.Empty, new TaskAttemptId(new TaskId("bogus", 1), 1)));
        }
    }
}
