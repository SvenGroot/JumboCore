﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test;

[TestFixture]
public class RawComparerTests
{
    [Test]
    public void TestIndexedQuickSort()
    {
        const int count = 1000;
        List<int> values = new List<int>(count);
        Random rnd = new Random();

        byte[] buffer;
        RecordIndexEntry[] index = new RecordIndexEntry[count];
        using (MemoryStream stream = new MemoryStream(count * sizeof(int)))
        using (BinaryWriter writer = new BinaryWriter(stream))
        {
            for (int x = 0; x < count; ++x)
            {
                int value = rnd.Next();
                values.Add(value);
                index[x] = new RecordIndexEntry((int)stream.Position, sizeof(int));
                writer.Write(value);
                writer.Flush();
            }
            writer.Flush();
            buffer = stream.ToArray();
        }


        values.Sort();
        IndexedQuicksort.Sort(index, buffer, RawComparer<int>.CreateComparer());

        var result = index.Select(e => LittleEndianBitConverter.ToInt32(buffer, e.Offset)).ToList();
        Assert.That(result, Is.EqualTo(values).AsCollection);

    }

    [Test]
    public void TestInt32Comparer()
    {
        TestComparer(10, 100);
    }

    [Test]
    public void TestPairComparer()
    {
        TestComparer(Pair.MakePair(5, 10), Pair.MakePair(10, 5));

        // Make sure the value isn't used by checking that two pairs with identical keys but different values compare equal.
        byte[] buffer;
        int secondOffset;
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        {
            ValueWriter<Pair<int, int>>.WriteValue(Pair.MakePair(10, 5), writer);
            secondOffset = (int)stream.Length;
            ValueWriter<Pair<int, int>>.WriteValue(Pair.MakePair(10, 10), writer);
            buffer = stream.ToArray();
        }

        Assert.That(RawComparer<Pair<int, int>>.Comparer.Compare(buffer, 0, secondOffset, buffer, secondOffset, secondOffset), Is.EqualTo(0));
    }

    [Test]
    public void TestUtf8StringComparer()
    {
        TestComparer(new Utf8String("aardvark"), new Utf8String("zebra"));
    }

    private void TestComparer<T>(T small, T large)
    {
        Assert.That(RawComparer<T>.Comparer, Is.Not.Null);
        byte[] buffer;
        int largeOffset;
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        {
            ValueWriter<T>.WriteValue(small, writer);
            largeOffset = (int)stream.Length;
            ValueWriter<T>.WriteValue(large, writer);
            buffer = stream.ToArray();
        }

        Assert.That(RawComparer<T>.Comparer.Compare(buffer, 0, largeOffset, buffer, largeOffset, buffer.Length - largeOffset), Is.LessThan(0));
        Assert.That(RawComparer<T>.Comparer.Compare(buffer, largeOffset, buffer.Length - largeOffset, buffer, 0, largeOffset), Is.GreaterThan(0));
        Assert.That(RawComparer<T>.Comparer.Compare(buffer, 0, largeOffset, buffer, 0, largeOffset), Is.EqualTo(0));
        Assert.That(RawComparer<T>.Comparer.Compare(buffer, largeOffset, buffer.Length - largeOffset, buffer, largeOffset, buffer.Length - largeOffset), Is.EqualTo(0));
    }
}
