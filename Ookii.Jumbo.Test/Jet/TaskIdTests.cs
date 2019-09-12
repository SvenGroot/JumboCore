﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Jet;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Ookii.Jumbo.Test.Jet
{
    [TestFixture]
    public class TaskIdTests
    {
        [Test]
        public void TestConstructionFromString()
        {
            string taskId = "Test-003";
            TaskId target = new TaskId(taskId);

            Assert.AreEqual(taskId, target.ToString());
            Assert.AreEqual("Test", target.StageId);
            Assert.AreEqual(3, target.TaskNumber);
            Assert.IsNull(target.ParentTaskId);
        }

        [Test]
        public void TestConstructionFromCompoundString()
        {
            string taskId = "Parent-002.Child-007.SecondChild-001";
            TaskId target = new TaskId(taskId);

            Assert.AreEqual(taskId, target.ToString());
            Assert.AreEqual("SecondChild", target.StageId);
            Assert.AreEqual(1, target.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId);
            Assert.AreEqual("Parent-002.Child-007", target.ParentTaskId.ToString());
            Assert.AreEqual("Child", target.ParentTaskId.StageId);
            Assert.AreEqual(7, target.ParentTaskId.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId.ParentTaskId);
            Assert.AreEqual("Parent-002", target.ParentTaskId.ParentTaskId.ToString());
            Assert.AreEqual("Parent", target.ParentTaskId.ParentTaskId.StageId);
            Assert.AreEqual(2, target.ParentTaskId.ParentTaskId.TaskNumber);
            Assert.IsNull(target.ParentTaskId.ParentTaskId.ParentTaskId);
        }

        [Test]
        public void TestConstructionFromStageIdAndTaskNumber()
        {
            string stageId = "Test";
            int taskNumber = 3;
            TaskId target = new TaskId(stageId, taskNumber);

            Assert.AreEqual("Test-003", target.ToString());
            Assert.AreEqual(stageId, target.StageId);
            Assert.AreEqual(taskNumber, target.TaskNumber);
            Assert.IsNull(target.ParentTaskId);
        }

        [Test]
        public void TestConstructionFromStringWithParent()
        {
            TaskId parentTaskId = new TaskId("Parent-002.Child-007");
            TaskId target = new TaskId(parentTaskId, "SecondChild-001");

            Assert.AreEqual("Parent-002.Child-007.SecondChild-001", target.ToString());
            Assert.AreEqual("SecondChild", target.StageId);
            Assert.AreEqual(1, target.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId);
            Assert.AreEqual("Parent-002.Child-007", target.ParentTaskId.ToString());
            Assert.AreEqual("Child", target.ParentTaskId.StageId);
            Assert.AreEqual(7, target.ParentTaskId.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId.ParentTaskId);
            Assert.AreEqual("Parent-002", target.ParentTaskId.ParentTaskId.ToString());
            Assert.AreEqual("Parent", target.ParentTaskId.ParentTaskId.StageId);
            Assert.AreEqual(2, target.ParentTaskId.ParentTaskId.TaskNumber);
            Assert.IsNull(target.ParentTaskId.ParentTaskId.ParentTaskId);
        }

        [Test]
        public void TestConstructionFromStageIdAndTaskNumberWithParent()
        {
            TaskId parentTaskId = new TaskId("Parent-002.Child-007");
            TaskId target = new TaskId(parentTaskId, "SecondChild", 4);

            Assert.AreEqual("Parent-002.Child-007.SecondChild-004", target.ToString());
            Assert.AreEqual("SecondChild", target.StageId);
            Assert.AreEqual(4, target.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId);
            Assert.AreEqual("Parent-002.Child-007", target.ParentTaskId.ToString());
            Assert.AreEqual("Child", target.ParentTaskId.StageId);
            Assert.AreEqual(7, target.ParentTaskId.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId.ParentTaskId);
            Assert.AreEqual("Parent-002", target.ParentTaskId.ParentTaskId.ToString());
            Assert.AreEqual("Parent", target.ParentTaskId.ParentTaskId.StageId);
            Assert.AreEqual(2, target.ParentTaskId.ParentTaskId.TaskNumber);
            Assert.IsNull(target.ParentTaskId.ParentTaskId.ParentTaskId);
        }

        [Test]
        public void TestSerialization()
        {
            string taskId = "Parent-002.Child-007.SecondChild-001";
            TaskId original = new TaskId(taskId);
            TaskId target;

            BinaryFormatter formatter = new BinaryFormatter();
            using( MemoryStream stream = new MemoryStream() )
            {
                formatter.Serialize(stream, original);
                stream.Position = 0;
                target = (TaskId)formatter.Deserialize(stream);
            }

            Assert.AreEqual(taskId, target.ToString());
            Assert.AreEqual("SecondChild", target.StageId);
            Assert.AreEqual(1, target.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId);
            Assert.AreEqual("Parent-002.Child-007", target.ParentTaskId.ToString());
            Assert.AreEqual("Child", target.ParentTaskId.StageId);
            Assert.AreEqual(7, target.ParentTaskId.TaskNumber);
            Assert.IsNotNull(target.ParentTaskId.ParentTaskId);
            Assert.AreEqual("Parent-002", target.ParentTaskId.ParentTaskId.ToString());
            Assert.AreEqual("Parent", target.ParentTaskId.ParentTaskId.StageId);
            Assert.AreEqual(2, target.ParentTaskId.ParentTaskId.TaskNumber);
            Assert.IsNull(target.ParentTaskId.ParentTaskId.ParentTaskId);            
        }
    }
}
