using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using NUnit.Framework;

#pragma warning disable SYSLIB0011 // BinaryFormatter is deprecated.

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class ExtendedCollectionTests
    {
        [Test]
        public void TestSerialization()
        {
            var test = new ExtendedCollection<int>() { 1, 2, 3, 4, 5 };
            BinaryFormatter formatter = new BinaryFormatter();
            using (MemoryStream stream = new MemoryStream())
            {
                formatter.Serialize(stream, test);
                stream.Position = 0;
                var test2 = (ExtendedCollection<int>)formatter.Deserialize(stream);
                CollectionAssert.AreEqual(test, test2);
            }
        }
    }
}
