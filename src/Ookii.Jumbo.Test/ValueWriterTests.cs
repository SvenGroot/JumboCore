using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test
{
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

        private void TestSerialization<T>(T expected)
        {
            using (MemoryStream stream = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(stream))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                ValueWriter<T>.WriteValue(expected, writer);
                writer.Flush();
                stream.Position = 0;
                T actual = ValueWriter<T>.ReadValue(reader);
                Assert.AreEqual(expected, actual);
            }
        }
    }
}
