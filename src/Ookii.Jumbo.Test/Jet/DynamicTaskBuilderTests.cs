﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Jobs.Builder;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Test.Jet;

[TestFixture]
public class DynamicTaskBuilderTests
{
    [Test]
    public void TestCreateDynamicTask()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        Action<RecordReader<int>, RecordWriter<int>, TaskContext> taskDelegate = TaskMethod;
        Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
        TaskContext context = CreateConfiguration(taskType);
        context.StageConfiguration.AddSetting("Factor", 2);
        ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
        List<int> data = Utilities.GenerateNumberData(10);
        List<int> result;
        using (EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data))
        using (ListRecordWriter<int> output = new ListRecordWriter<int>())
        {
            task.Run(input, output);
            result = output.List.ToList();
        }

        Assert.That(result, Is.EqualTo(data.Select(i => i * 2).ToList()).AsCollection);
    }

    [Test]
    public void TestCreateDynamicTaskNoContext()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        Action<RecordReader<int>, RecordWriter<int>> taskDelegate = TaskMethodNoContext;
        Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 0, RecordReuseMode.Allow);
        TaskContext context = CreateConfiguration(taskType);
        ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
        List<int> data = Utilities.GenerateNumberData(10);
        List<int> result;
        using (EnumerableRecordReader<int> input = new EnumerableRecordReader<int>(data))
        using (ListRecordWriter<int> output = new ListRecordWriter<int>())
        {
            task.Run(input, output);
            result = output.List.ToList();
        }

        Assert.That(result, Is.EqualTo(data.Select(i => i * 3).ToList()).AsCollection);
    }


    [Test]
    public void TestCreateDynamicTaskReturnType()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        Func<Utf8String, int, int, int> taskDelegate = AccumulateMethod;
        Type taskType = target.CreateDynamicTask(typeof(AccumulatorTask<Utf8String, int>).GetMethod("Accumulate", BindingFlags.NonPublic | BindingFlags.Instance), taskDelegate, 0, RecordReuseMode.Allow);
        TaskContext context = CreateConfiguration(taskType);
        AccumulatorTask<Utf8String, int> task = (AccumulatorTask<Utf8String, int>)JetActivator.CreateInstance(taskType, null, null, context);
        List<string> data = Utilities.GenerateDataWords(null, 100, 10);
        List<KeyValuePair<string, int>> result;
        using (EnumerableRecordReader<Pair<Utf8String, int>> input = new EnumerableRecordReader<Pair<Utf8String, int>>(data.Select(w => Pair.MakePair(new Utf8String(w), 1)), data.Count))
        using (ListRecordWriter<Pair<Utf8String, int>> output = new ListRecordWriter<Pair<Utf8String, int>>(true))
        {
            task.Run(input, output);
            result = output.List.Select(p => new KeyValuePair<string, int>(p.Key.ToString(), p.Value)).OrderBy(p => p.Key, StringComparer.Ordinal).ToList();
        }

        List<KeyValuePair<string, int>> expected = (from word in data
                                                    group word by word into g
                                                    select new KeyValuePair<string, int>(g.Key, g.Count())).OrderBy(r => r.Key, StringComparer.Ordinal).ToList();

        Assert.That(result, Is.EqualTo(expected).AsCollection);
    }

    [Test]
    public void TestCreateDynamicTaskSkipParameters()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        Action<RecordWriter<int>, TaskContext> taskDelegate = TaskMethodNoInput;
        Type taskType = target.CreateDynamicTask(typeof(ITask<int, int>).GetMethod("Run"), taskDelegate, 1, RecordReuseMode.Allow);
        TaskContext context = CreateConfiguration(taskType);
        context.StageConfiguration.AddSetting("Count", 6);
        ITask<int, int> task = (ITask<int, int>)JetActivator.CreateInstance(taskType, null, null, context);
        List<int> result;
        using (ListRecordWriter<int> output = new ListRecordWriter<int>())
        {
            task.Run(null, output);
            result = output.List.ToList();
        }

        Assert.That(result, Is.EqualTo(Enumerable.Range(0, 6).ToList()).AsCollection);
    }

    [Test]
    public void TestCreateDynamicTaskAllowRecordReuse()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        MethodInfo runMethod = typeof(ITask<int, int>).GetMethod("Run");
        // From attribute
        Type type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReuse, 0, RecordReuseMode.Default);
        VerifyRecordReuse(type, true);
        // With passthrough
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReusePassThrough, 0, RecordReuseMode.Default);
        VerifyRecordReuse(type, true, true);
        // Not allowed despite attribute
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodAllowRecordReuse, 0, RecordReuseMode.DoNotAllow);
        VerifyRecordReuse(type, false);
        // No attribute
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.Default);
        VerifyRecordReuse(type, false);
        // No attribute with mode
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.Allow);
        VerifyRecordReuse(type, true);
        // No attribute with mode (passthrough)
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.PassThrough);
        VerifyRecordReuse(type, true, true);
        // Mode used for lambda
        type = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)((input, output) => { }), 0, RecordReuseMode.Allow);
        VerifyRecordReuse(type, true);
    }

    [Test]
    public void TestCreateDynamicTaskCache()
    {
        DynamicTaskBuilder target = new DynamicTaskBuilder();
        MethodInfo runMethod = typeof(ITask<int, int>).GetMethod("Run");
        Type type1 = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.DoNotAllow);
        Type type2 = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.DoNotAllow);
        Type type3 = target.CreateDynamicTask(runMethod, (Action<RecordReader<int>, RecordWriter<int>>)TaskMethodNoContext, 0, RecordReuseMode.Allow);

        Assert.That(type1.Name, Is.EqualTo("TaskMethodNoContextTask"));
        Assert.That(type3.Name, Is.EqualTo("TaskMethodNoContextTask2"));
        Assert.That(type2, Is.EqualTo(type1));
        Assert.That(type3, Is.Not.EqualTo(type2));
    }

    public static void TaskMethod(RecordReader<int> input, RecordWriter<int> output, TaskContext context)
    {
        int factor = context.GetSetting("Factor", 0);
        output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
    }

    public static void TaskMethodNoContext(RecordReader<int> input, RecordWriter<int> output)
    {
        int factor = 3;
        output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
    }

    public static void TaskMethodNoInput(RecordWriter<int> output, TaskContext context)
    {
        int count = context.GetSetting("Count", 0);
        output.WriteRecords(Enumerable.Range(0, count));
    }

    public static int AccumulateMethod(Utf8String key, int value, int newValue)
    {
        return value + newValue;
    }

    [AllowRecordReuse]
    public static void TaskMethodAllowRecordReuse(RecordReader<int> input, RecordWriter<int> output)
    {
    }

    [AllowRecordReuse(PassThrough = true)]
    public static void TaskMethodAllowRecordReusePassThrough(RecordReader<int> input, RecordWriter<int> output)
    {
    }

    private static void TaskMethodNonPublic(RecordReader<int> input, RecordWriter<int> output)
    {
        int factor = 4;
        output.WriteRecords(input.EnumerateRecords().Select(i => i * factor));
    }

    private static void VerifyRecordReuse(Type type, bool allow, bool passthrough = false)
    {
        AllowRecordReuseAttribute attribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(type, typeof(AllowRecordReuseAttribute));
        if (allow)
        {
            Assert.That(attribute, Is.Not.Null);
            Assert.That(attribute.PassThrough, Is.EqualTo(passthrough));
        }
        else
        {
            Assert.That(attribute, Is.Null);
        }
    }

    private TaskContext CreateConfiguration(Type taskType)
    {
        JobConfiguration job = new JobConfiguration();
        StageConfiguration stage = job.AddStage("TestStage", taskType, 1, null);
        return new TaskContext(Guid.Empty, job, new TaskAttemptId(new TaskId(stage.StageId, 1), 1), stage, Path.GetTempPath(), "/fake");
    }
}
