// Copyright (c) Sven Groot (Ookii.org)
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Jet;

[TestFixture]
public class TaskIdTests
{
    [Test]
    public void TestConstructionFromString()
    {
        string taskId = "Test-003";
        TaskId target = new TaskId(taskId);

        Assert.That(target.ToString(), Is.EqualTo(taskId));
        Assert.That(target.StageId, Is.EqualTo("Test"));
        Assert.That(target.TaskNumber, Is.EqualTo(3));
        Assert.That(target.ParentTaskId, Is.Null);
    }

    [Test]
    public void TestConstructionFromCompoundString()
    {
        string taskId = "Parent-002.Child-007.SecondChild-001";
        TaskId target = new TaskId(taskId);

        Assert.That(target.ToString(), Is.EqualTo(taskId));
        Assert.That(target.StageId, Is.EqualTo("SecondChild"));
        Assert.That(target.TaskNumber, Is.EqualTo(1));
        Assert.That(target.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ToString(), Is.EqualTo("Parent-002.Child-007"));
        Assert.That(target.ParentTaskId.StageId, Is.EqualTo("Child"));
        Assert.That(target.ParentTaskId.TaskNumber, Is.EqualTo(7));
        Assert.That(target.ParentTaskId.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ParentTaskId.ToString(), Is.EqualTo("Parent-002"));
        Assert.That(target.ParentTaskId.ParentTaskId.StageId, Is.EqualTo("Parent"));
        Assert.That(target.ParentTaskId.ParentTaskId.TaskNumber, Is.EqualTo(2));
        Assert.That(target.ParentTaskId.ParentTaskId.ParentTaskId, Is.Null);
    }

    [Test]
    public void TestConstructionFromStageIdAndTaskNumber()
    {
        string stageId = "Test";
        int taskNumber = 3;
        TaskId target = new TaskId(stageId, taskNumber);

        Assert.That(target.ToString(), Is.EqualTo("Test-003"));
        Assert.That(target.StageId, Is.EqualTo(stageId));
        Assert.That(target.TaskNumber, Is.EqualTo(taskNumber));
        Assert.That(target.ParentTaskId, Is.Null);
    }

    [Test]
    public void TestConstructionFromStringWithParent()
    {
        TaskId parentTaskId = new TaskId("Parent-002.Child-007");
        TaskId target = new TaskId(parentTaskId, "SecondChild-001");

        Assert.That(target.ToString(), Is.EqualTo("Parent-002.Child-007.SecondChild-001"));
        Assert.That(target.StageId, Is.EqualTo("SecondChild"));
        Assert.That(target.TaskNumber, Is.EqualTo(1));
        Assert.That(target.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ToString(), Is.EqualTo("Parent-002.Child-007"));
        Assert.That(target.ParentTaskId.StageId, Is.EqualTo("Child"));
        Assert.That(target.ParentTaskId.TaskNumber, Is.EqualTo(7));
        Assert.That(target.ParentTaskId.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ParentTaskId.ToString(), Is.EqualTo("Parent-002"));
        Assert.That(target.ParentTaskId.ParentTaskId.StageId, Is.EqualTo("Parent"));
        Assert.That(target.ParentTaskId.ParentTaskId.TaskNumber, Is.EqualTo(2));
        Assert.That(target.ParentTaskId.ParentTaskId.ParentTaskId, Is.Null);
    }

    [Test]
    public void TestConstructionFromStageIdAndTaskNumberWithParent()
    {
        TaskId parentTaskId = new TaskId("Parent-002.Child-007");
        TaskId target = new TaskId(parentTaskId, "SecondChild", 4);

        Assert.That(target.ToString(), Is.EqualTo("Parent-002.Child-007.SecondChild-004"));
        Assert.That(target.StageId, Is.EqualTo("SecondChild"));
        Assert.That(target.TaskNumber, Is.EqualTo(4));
        Assert.That(target.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ToString(), Is.EqualTo("Parent-002.Child-007"));
        Assert.That(target.ParentTaskId.StageId, Is.EqualTo("Child"));
        Assert.That(target.ParentTaskId.TaskNumber, Is.EqualTo(7));
        Assert.That(target.ParentTaskId.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ParentTaskId.ToString(), Is.EqualTo("Parent-002"));
        Assert.That(target.ParentTaskId.ParentTaskId.StageId, Is.EqualTo("Parent"));
        Assert.That(target.ParentTaskId.ParentTaskId.TaskNumber, Is.EqualTo(2));
        Assert.That(target.ParentTaskId.ParentTaskId.ParentTaskId, Is.Null);
    }

    [Test]
    public void TestSerialization()
    {
        string taskId = "Parent-002.Child-007.SecondChild-001";
        TaskId original = new TaskId(taskId);
        TaskId target;

        using (var stream = new MemoryStream())
        using (var writer = new BinaryWriter(stream))
        using (var reader = new BinaryReader(stream))
        {
            ValueWriter.WriteValue(original, writer);
            stream.Position = 0;
            target = ValueWriter<TaskId>.ReadValue(reader);
        }

        Assert.That(target.ToString(), Is.EqualTo(taskId));
        Assert.That(target.StageId, Is.EqualTo("SecondChild"));
        Assert.That(target.TaskNumber, Is.EqualTo(1));
        Assert.That(target.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ToString(), Is.EqualTo("Parent-002.Child-007"));
        Assert.That(target.ParentTaskId.StageId, Is.EqualTo("Child"));
        Assert.That(target.ParentTaskId.TaskNumber, Is.EqualTo(7));
        Assert.That(target.ParentTaskId.ParentTaskId, Is.Not.Null);
        Assert.That(target.ParentTaskId.ParentTaskId.ToString(), Is.EqualTo("Parent-002"));
        Assert.That(target.ParentTaskId.ParentTaskId.StageId, Is.EqualTo("Parent"));
        Assert.That(target.ParentTaskId.ParentTaskId.TaskNumber, Is.EqualTo(2));
        Assert.That(target.ParentTaskId.ParentTaskId.ParentTaskId, Is.Null);
    }
}
