﻿// Copyright (c) Sven Groot (Ookii.org)
using System.Text;
using NUnit.Framework;

namespace Ookii.Jumbo.Test;

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
        Crc32Checksum target = new Crc32Checksum();
        Assert.That(target.Value, Is.EqualTo(0));
        Assert.That(target.ValueUInt32, Is.EqualTo(0));
    }

    [Test]
    public void TestUpdateManaged()
    {
        Crc32Checksum target = new Crc32Checksum();
        target.Update(_testData);
        Assert.That(target.Value, Is.EqualTo(_expectedChecksum));
        Assert.That(target.ValueUInt32, Is.EqualTo(_expectedChecksum));
    }

    [Test]
    public void TestUpdatePartialManaged()
    {
        Crc32Checksum target = new Crc32Checksum();
        target.Update(_testData, 0, 500);
        target.Update(_testData, 500, _testData.Length - 500);
        Assert.That(target.Value, Is.EqualTo(_expectedChecksum));
        Assert.That(target.ValueUInt32, Is.EqualTo(_expectedChecksum));
    }
}
