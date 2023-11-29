// Copyright (c) Sven Groot (Ookii.org)
using NUnit.Framework;

namespace Ookii.Jumbo.Test;

[TestFixture]
public class UnmanagedBufferTests
{
    [Test]
    public void TestCopyCircular()
    {
        using (UnmanagedBuffer target = new UnmanagedBuffer(1024))
        {
            byte[] expected = Utilities.GenerateData(512);
            byte[] actual = new byte[512];

            long index = UnmanagedBuffer.CopyCircular(expected, 0, target, 0, expected.Length);

            Assert.That(index, Is.EqualTo(512));
            UnmanagedBuffer.CopyCircular(target, 0, actual, 0, 512);
            Assert.That(actual, Is.EqualTo(expected).AsCollection);
            Assert.That(Utilities.CompareArray(expected, 0, actual, 0, 512), Is.True);

            index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);

            Assert.That(index, Is.EqualTo(0));
            UnmanagedBuffer.CopyCircular(target, 512, actual, 0, 512);
            Assert.That(actual, Is.EqualTo(expected).AsCollection);
            index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);
            index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, 256);
            Assert.That(index, Is.EqualTo(768));
            index = UnmanagedBuffer.CopyCircular(expected, 0, target, index, expected.Length);
            Assert.That(index, Is.EqualTo(256));

            UnmanagedBuffer.CopyCircular(target, 768, actual, 0, actual.Length);
            Assert.That(actual, Is.EqualTo(expected).AsCollection);
        }
    }
}
