// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs;

[TestFixture]
public class PacketTests
{
    private Random _rnd = new Random();

    [Test]
    public void TestConstructor()
    {
        Packet packet = new Packet();
        Assert.That(packet.Size, Is.EqualTo(0));
        Assert.That(packet.IsLastPacket, Is.False);
        Assert.That(packet.Checksum, Is.EqualTo(0));
    }

    [Test]
    public void TestConstructorData()
    {
        long checksum;
        byte[] data = GenerateData(500, out checksum);
        Packet target = new Packet(data, 500, 1, true);
        Assert.That(target.IsLastPacket, Is.True);
        Assert.That(target.Size, Is.EqualTo(500));
        Assert.That(target.Checksum, Is.EqualTo(checksum));
        Assert.That(target.SequenceNumber, Is.EqualTo(1));
    }

    [Test]
    public void TestRead()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        Packet packet = new Packet();
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            writer.Write((uint)checksum);
            writer.Write(5000);
            writer.Write(true);
            writer.Write(2L); // sequence
            writer.Write(data, 0, 5000);

            stream.Position = 0;
            packet.Read(reader, PacketFormatOption.Default, true);
        }
        Assert.That(packet.Checksum, Is.EqualTo(checksum));
        Assert.That(packet.Size, Is.EqualTo(5000));
        Assert.That(packet.SequenceNumber, Is.EqualTo(2L));
        Assert.That(packet.IsLastPacket, Is.True);
    }

    [Test]
    public void TestReadNoSequenceNumber()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        Packet packet = new Packet();
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            writer.Write((uint)checksum);
            writer.Write(5000);
            writer.Write(true);
            writer.Write(data, 0, 5000);

            stream.Position = 0;
            packet.Read(reader, PacketFormatOption.NoSequenceNumber, true);
        }
        Assert.That(packet.Checksum, Is.EqualTo(checksum));
        Assert.That(packet.Size, Is.EqualTo(5000));
        Assert.That(packet.SequenceNumber, Is.EqualTo(0L));
        Assert.That(packet.IsLastPacket, Is.True);
    }

    [Test]
    public void TestReadChecksumOnly()
    {
        long checksum;
        byte[] data = GenerateData(Packet.PacketSize, out checksum);
        Packet packet = new Packet();
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            // Test two packets because Read uses stream length to set IsLastPacket if checksumOnly is true.
            writer.Write((uint)checksum);
            writer.Write(data, 0, Packet.PacketSize);
            long checksum2;
            data = GenerateData(5000, out checksum2);
            writer.Write((uint)checksum2);
            writer.Write(data, 0, 5000);

            stream.Position = 0;
            packet.Read(reader, PacketFormatOption.ChecksumOnly, true);
            Assert.That(packet.Checksum, Is.EqualTo(checksum));
            Assert.That(packet.Size, Is.EqualTo(Packet.PacketSize));
            Assert.That(packet.IsLastPacket, Is.False);
            packet.Read(reader, PacketFormatOption.ChecksumOnly, true);
            Assert.That(packet.Checksum, Is.EqualTo(checksum2));
            Assert.That(packet.Size, Is.EqualTo(5000));
            Assert.That(packet.IsLastPacket, Is.True);
        }
    }

    [Test]
    public void TestWrite()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            Packet packet = new Packet(data, 5000, 2, true);
            packet.Write(writer, PacketFormatOption.Default);

            stream.Position = 0;
            Assert.That(reader.ReadUInt32(), Is.EqualTo(checksum));
            Assert.That(reader.ReadInt32(), Is.EqualTo(5000));
            Assert.That(reader.ReadBoolean(), Is.EqualTo(true));
            Assert.That(reader.ReadInt64(), Is.EqualTo(2L));
            byte[] readData = new byte[5000];
            Assert.That(reader.Read(readData, 0, 5000), Is.EqualTo(5000));
            Assert.That(Utilities.CompareArray(data, 0, readData, 0, 5000), Is.True);
            Assert.That(stream.Position, Is.EqualTo(stream.Length));
        }
    }

    [Test]
    public void TestWriteChecksumOnly()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            Packet packet = new Packet(data, 5000, 1, true);
            packet.Write(writer, PacketFormatOption.ChecksumOnly);

            stream.Position = 0;
            Assert.That(reader.ReadUInt32(), Is.EqualTo(checksum));
            byte[] readData = new byte[5000];
            Assert.That(reader.Read(readData, 0, 5000), Is.EqualTo(5000));
            Assert.That(Utilities.CompareArray(data, 0, readData, 0, 5000), Is.True);
            Assert.That(stream.Position, Is.EqualTo(stream.Length));
        }
    }

    [Test]
    public void TestWriteNoSequenceNumber()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        using (MemoryStream stream = new MemoryStream())
        using (BinaryWriter writer = new BinaryWriter(stream))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            Packet packet = new Packet(data, 5000, 2, true);
            packet.Write(writer, PacketFormatOption.NoSequenceNumber);

            stream.Position = 0;
            Assert.That(reader.ReadUInt32(), Is.EqualTo(checksum));
            Assert.That(reader.ReadInt32(), Is.EqualTo(5000));
            Assert.That(reader.ReadBoolean(), Is.EqualTo(true));
            byte[] readData = new byte[5000];
            Assert.That(reader.Read(readData, 0, 5000), Is.EqualTo(5000));
            Assert.That(Utilities.CompareArray(data, 0, readData, 0, 5000), Is.True);
            Assert.That(stream.Position, Is.EqualTo(stream.Length));
        }
    }

    [Test]
    public void TestWriteDataOnly()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        using (MemoryStream stream = new MemoryStream())
        using (BinaryReader reader = new BinaryReader(stream))
        {
            Packet packet = new Packet(data, 5000, 1, true);
            packet.WriteDataOnly(stream);

            stream.Position = 0;
            byte[] readData = new byte[5000];
            Assert.That(reader.Read(readData, 0, 5000), Is.EqualTo(5000));
            Assert.That(Utilities.CompareArray(data, 0, readData, 0, 5000), Is.True);
            Assert.That(stream.Position, Is.EqualTo(stream.Length));
        }
    }

    [Test]
    public void TestCopyTo()
    {
        long checksum;
        byte[] data = GenerateData(5000, out checksum);
        Packet packet = new Packet(data, 5000, 1, true);
        byte[] readData = new byte[5000];
        packet.CopyTo(0, readData, 0, 5000);
        Assert.That(Utilities.CompareArray(data, 0, readData, 0, 5000), Is.True);
        packet.CopyTo(500, readData, 100, 1000);
        Assert.That(Utilities.CompareArray(data, 500, readData, 100, 1000), Is.True);
    }

    [Test]
    public void TestEquals()
    {
        long checksum;
        byte[] data = GenerateData(Packet.PacketSize, out checksum);
        Packet packet1 = new Packet(data, Packet.PacketSize, 1, false);
        Packet packet2 = new Packet(data, Packet.PacketSize, 1, false);
        Assert.That(packet2, Is.EqualTo(packet1));
        packet2 = new Packet(data, Packet.PacketSize, 1, true);
        Assert.That(packet2, Is.Not.EqualTo(packet1));
        packet2 = new Packet(data, Packet.PacketSize - 1, 1, true);
        Assert.That(packet2, Is.Not.EqualTo(packet1));
        byte[] data2 = GenerateData(Packet.PacketSize, out checksum);
        packet2 = new Packet(data2, Packet.PacketSize, 1, false);
        Assert.That(packet2, Is.Not.EqualTo(packet1));
    }

    private byte[] GenerateData(int size, out long checksum)
    {
        byte[] data = new byte[Packet.PacketSize]; // Intentially not size so the size paremeter for the constructor can be tested.
        _rnd.NextBytes(data);
        Crc32Checksum checksum2 = new Crc32Checksum();
        checksum2.Update(data, 0, size);
        checksum = checksum2.Value;
        return data;
    }
}
