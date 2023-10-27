using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test;

[TestFixture]
public class ValueWriterTests
{
    [Test]
    public void TestTupleSerialization()
    {
        TestSerialization(Tuple.Create("test"));
        TestSerialization(Tuple.Create("test", 1));
        TestSerialization(Tuple.Create("test", 1, 2));
        TestSerialization(Tuple.Create("test", 1, 2, 3));
        TestSerialization(Tuple.Create("test", 1, 2, 3, 4L));
        TestSerialization(Tuple.Create("test", 1, 2, 3, 4L, true));
        TestSerialization(Tuple.Create("test", 1, 2, 3, 4L, true, DateTime.UtcNow));
        TestSerialization(Tuple.Create("test", 1, 2, 3, 4L, true, DateTime.UtcNow, 5.0f));
    }

    [Test]
    public void TestValueTupleSerialization()
    {
        TestSerialization(("test"));
        TestSerialization(("test", 1));
        TestSerialization(("test", 1, 2));
        TestSerialization(("test", 1, 2, 3));
        TestSerialization(("test", 1, 2, 3, 4L));
        TestSerialization(("test", 1, 2, 3, 4L, true));
        TestSerialization(("test", 1, 2, 3, 4L, true, DateTime.UtcNow));
        TestSerialization(("test", 1, 2, 3, 4L, true, DateTime.UtcNow, 5.0f));
    }

    [Test]
    public void TestEnumSerialization()
    {
        TestSerialization(DayOfWeek.Friday);
    }

    [Test]
    public void TestByteArraySerialization()
    {
        TestSerialization(new byte[] { 1, 2, 3, 4, 5 });
    }

    [Test]
    public void TestArraySerialization()
    {
        TestSerialization(new int[] { 1, 2, 3, 4, 5 });
    }

    [Test]
    public void TestPolymorphic()
    {
        BaseClass expected = new DerivedClass1 { Test = 42 };
        var actual = SerializeDeserialize(expected);
        Assert.AreEqual(42, ((DerivedClass1)actual).Test);

        expected = new DerivedClass2 { Test = "foo" };
        actual = SerializeDeserialize(expected);
        Assert.AreEqual("foo", ((DerivedClass2)actual).Test);
    }

    private static void TestSerialization<T>(T expected)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);
        ValueWriter<T>.WriteValue(expected, writer);
        writer.Flush();
        stream.Position = 0;
        T actual = ValueWriter<T>.ReadValue(reader);
        Assert.AreEqual(expected, actual);
    }

    private static void TestSerialization<T>(T[] expected)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);
        ValueWriter<T[]>.WriteValue(expected, writer);
        writer.Flush();
        stream.Position = 0;
        T[] actual = ValueWriter<T[]>.ReadValue(reader);
        CollectionAssert.AreEqual(expected, actual);
    }

    private T SerializeDeserialize<T>(T value)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        using var reader = new BinaryReader(stream);
        ValueWriter<T>.WriteValue(value, writer);
        writer.Flush();
        stream.Position = 0;
        return ValueWriter<T>.ReadValue(reader);
    }

}

[ValueWriter(typeof(PolymorphicValueWriter<BaseClass>))]
[WritableDerivedType(typeof(DerivedClass1))]
[WritableDerivedType(typeof(DerivedClass2))]
public abstract class BaseClass
{
}

[GeneratedWritable]
public partial class DerivedClass1 : BaseClass
{
    public int Test { get; set; }
}

[GeneratedWritable]
public partial class DerivedClass2 : BaseClass
{
    public string Test { get; set; }
}
