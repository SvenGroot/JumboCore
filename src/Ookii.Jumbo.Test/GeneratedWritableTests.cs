// Copyright (c) Sven Groot (Ookii.org)
#nullable enable

using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class GeneratedWritableTests
    {
        [Test]
        public void TestSerialization()
        {
            TestClass expected = new TestClass(42)
            {
                StringProperty = "Hello",
                AnotherStringProperty = null,
                BooleanProperty = true,
                NullableProperty = 42,
                WritableProperty = new Utf8String("47"),
                AnotherWritableProperty = null,
                DateProperty = DateTime.UtcNow,
                ByteArrayProperty = new byte[] { 1, 2, 3, 4 },
                IntArrayProperty = new int[] { 1234, 567457, 545643, 8786, 5613 },
                Ignored = 42,
                ValueWriterProperty = new TestStruct() { Value1 = 10, Value2 = 20 },
                EnumProperty = DayOfWeek.Friday
            };
            byte[] data;
            using (var stream = new MemoryStream())
            {
                using (var writer = new BinaryWriter(stream))
                {
                    expected.Write(writer);
                }

                data = stream.ToArray();
            }

            var actual = WritableUtility.GetUninitializedWritable<TestClass>();
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReader(stream))
            {
                actual.Read(reader);
            }

            Assert.AreEqual(expected.StringProperty, actual.StringProperty);
            Assert.AreEqual(expected.AnotherStringProperty, actual.AnotherStringProperty);
            Assert.AreEqual(expected.IntProperty, actual.IntProperty);
            Assert.AreEqual(expected.NullableProperty, actual.NullableProperty);
            Assert.AreEqual(expected.BooleanProperty, actual.BooleanProperty);
            Assert.AreEqual(expected.WritableProperty, actual.WritableProperty);
            Assert.AreEqual(expected.AnotherWritableProperty, actual.AnotherWritableProperty);
            Assert.AreEqual(expected.DateProperty, actual.DateProperty);
            Assert.IsTrue(Utilities.CompareList(expected.ByteArrayProperty, actual.ByteArrayProperty));
            Assert.IsTrue(Utilities.CompareList(expected.IntArrayProperty, actual.IntArrayProperty));
            Assert.AreEqual(0, actual.Ignored); // Not serialized
            Assert.AreEqual(10, actual.ValueWriterProperty.Value1);
            Assert.AreEqual(20, actual.ValueWriterProperty.Value2);
            Assert.AreEqual(DayOfWeek.Friday, actual.EnumProperty);
        }

        [Test]
        public void TestNotNullException()
        {
            Assert.Throws(typeof(ArgumentNullException), () =>
            {
                TestClass expected = new TestClass();
                using (MemoryStream stream = new MemoryStream())
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Fails because StringProperty is null.
                    expected.Write(writer);
                }
            });
        }
    }

    [GeneratedWritable]
    partial class TestClass
    {
        public TestClass()
        {
        }

        public TestClass(int n)
        {
            IntProperty = n;
        }

        [WritableNotNull]
        public string StringProperty { get; set; } = default!;
        public string? AnotherStringProperty { get; set; }
        public int IntProperty { get; private set; }
        public int? NullableProperty { get; set; }
        public bool BooleanProperty { get; set; }
        public Utf8String? WritableProperty { get; set; }
        public Utf8String? AnotherWritableProperty { get; set; }
        public DateTime DateProperty { get; set; }
        public byte[]? ByteArrayProperty { get; set; }
        public int[]? IntArrayProperty { get; set; }
        [WritableIgnore]
        public int Ignored { get; set; }
        public TestStruct ValueWriterProperty { get; set; }
        public DayOfWeek EnumProperty { get; set; }
    }

    [ValueWriter(typeof(TestStructWriter))]
    struct TestStruct
    {
        public int Value1 { get; set; }
        public int Value2 { get; set; }
    }

    class TestStructWriter : IValueWriter<TestStruct>
    {

        public void Write(TestStruct value, BinaryWriter writer)
        {
            writer.Write(value.Value1);
            writer.Write(value.Value2);
        }

        public TestStruct Read(BinaryReader reader)
        {
            int value1 = reader.ReadInt32();
            int value2 = reader.ReadInt32();
            return new TestStruct() { Value1 = value1, Value2 = value2 };
        }
    }
}
