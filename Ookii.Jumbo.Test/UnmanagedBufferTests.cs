// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class UnmanagedBufferTests
    {
        [Test]
        public void TestCopyCircular()
        {
            using( UnmanagedBuffer target = new UnmanagedBuffer(1024) )
            {
                byte[] expected = Utilities.GenerateData(512);
                byte[] actual = new byte[512];

                long index = UnmanagedBuffer.CopyCircular(expected, 0, target, 0, expected.Length);

                Assert.AreEqual(512, index);
                UnmanagedBuffer.CopyCircular(target, 0, actual, 0, 512);
                CollectionAssert.AreEqual(expected, actual);
                Assert.IsTrue(Utilities.CompareArray(expected, 0, actual, 0, 512));

                index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);

                Assert.AreEqual(0, index);
                UnmanagedBuffer.CopyCircular(target, 512, actual, 0, 512);
                CollectionAssert.AreEqual(expected, actual);
                index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);
                index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, 256);
                Assert.AreEqual(768, index);
                index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);
                Assert.AreEqual(256, index);

                UnmanagedBuffer.CopyCircular(target, 768, actual, 0, actual.Length);
                CollectionAssert.AreEqual(expected, actual);
            }
        }
    }
}
