// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class Crc32Tests
    {
        private static readonly byte[] _testData = Encoding.ASCII.GetBytes("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer bibendum, turpis vestibulum mattis venenatis, mauris lacus cursus urna, eget vulputate lacus ligula sit amet nisl. Integer eu ligula a ipsum luctus commodo. Phasellus tempor sagittis neque, in sagittis nunc fringilla eget. Sed non pulvinar lorem. Donec vel eros eu odio malesuada eleifend. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Sed vestibulum libero dolor. Nam nec neque sapien, eget vestibulum turpis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Duis urna nunc, ultricies ut auctor at, facilisis nec libero. Nulla eget consequat augue. Quisque vestibulum molestie nulla eu dictum. Quisque in sapien a mi ultrices vestibulum at non mauris. Sed vitae eros nulla, in vulputate dolor. Aliquam non urna nisi. Etiam dui massa, volutpat fermentum vestibulum in, mattis a nisl. Quisque vel felis ac eros suscipit accumsan. Cras tempor sed.");
        private const uint _expectedChecksum = 0x2CE2577E;

        //[OneTimeSetUp]
        //public void SetUp()
        //{
        //    log4net.Config.BasicConfigurator.Configure();
        //}

        [Test]
        public void TestConstructor()
        {
            Crc32 target = new Crc32();
            Assert.AreEqual(0, target.Value);
            Assert.AreEqual(0, target.ValueUInt32);
        }

        [Test]
        public void TestUpdateNative()
        {
            Crc32.UseNativeCode = true;
            Crc32 target = new Crc32();
            target.Update(_testData);
            Assert.AreEqual(_expectedChecksum, target.Value);
            Assert.AreEqual(_expectedChecksum, target.ValueUInt32);
            if( !Crc32.UseNativeCode )
                Assert.Inconclusive("The native code CRC32 algorithm could not be used.");
        }

        [Test]
        public void TestUpdateSpeed()
        {
            var native = TimeCrc(true);
            var managed = TimeCrc(false);
            Assert.Less(native.TotalSeconds, managed.TotalSeconds);
        }

        private TimeSpan TimeCrc(bool useNativeCode)
        {
            Crc32.UseNativeCode = useNativeCode;
            Crc32 target = new Crc32();
            Stopwatch sw = Stopwatch.StartNew();
            for (int x = 0; x < 1000000; ++x)
            {
                target.Update(_testData);
            }

            sw.Stop();
            TestContext.Progress.WriteLine("Crc: {0:x}, elapsed: {1}", target.Value, sw.Elapsed);
            TestContext.Progress.WriteLine(sw.Elapsed);
            if (useNativeCode && !Crc32.UseNativeCode)
                Assert.Inconclusive("The native code CRC32 algorithm could not be used.");
            return sw.Elapsed;
        }

        [Test]
        public void TestUpdateManaged()
        {
            Crc32.UseNativeCode = false;
            Crc32 target = new Crc32();
            target.Update(_testData);
            Assert.AreEqual(_expectedChecksum, target.Value);
            Assert.AreEqual(_expectedChecksum, target.ValueUInt32);

            Crc32.UseNativeCode = true; // Set it back so DFS tests will use the native version if possible.
        }

        [Test]
        public void TestUpdatePartialNative()
        {
            Crc32.UseNativeCode = true;
            Crc32 target = new Crc32();
            target.Update(_testData, 0, 500);
            target.Update(_testData, 500, _testData.Length - 500);
            Assert.AreEqual(_expectedChecksum, target.Value);
            Assert.AreEqual(_expectedChecksum, target.ValueUInt32);
            if( !Crc32.UseNativeCode )
                Assert.Inconclusive("The native code CRC32 algorithm could not be used.");
        }

        [Test]
        public void TestUpdatePartialManaged()
        {
            Crc32.UseNativeCode = false;
            Crc32 target = new Crc32();
            target.Update(_testData, 0, 500);
            target.Update(_testData, 500, _testData.Length - 500);
            Assert.AreEqual(_expectedChecksum, target.Value);
            Assert.AreEqual(_expectedChecksum, target.ValueUInt32);

            Crc32.UseNativeCode = true; // Set it back so DFS tests will use the native version if possible.
        }    
    }
}
