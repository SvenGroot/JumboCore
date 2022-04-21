// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Dfs;

namespace Ookii.Jumbo.Test.Dfs
{
    [TestFixture]
    public class PacketTests
    {
        private Random _rnd = new Random();

        [Test]
        public void TestConstructor()
        {
            Packet packet = new Packet();
            Assert.AreEqual(0, packet.Size);
            Assert.IsFalse(packet.IsLastPacket);
            Assert.AreEqual(0, packet.Checksum);
        }

        [Test]
        public void TestConstructorData()
        {
            long checksum;
            byte[] data = GenerateData(500, out checksum);
            Packet target = new Packet(data, 500, 1, true);
            Assert.IsTrue(target.IsLastPacket);
            Assert.AreEqual(500, target.Size);
            Assert.AreEqual(checksum, target.Checksum);
            Assert.AreEqual(1, target.SequenceNumber);
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
            Assert.AreEqual(checksum, packet.Checksum);
            Assert.AreEqual(5000, packet.Size);
            Assert.AreEqual(2L, packet.SequenceNumber);
            Assert.IsTrue(packet.IsLastPacket);
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
            Assert.AreEqual(checksum, packet.Checksum);
            Assert.AreEqual(5000, packet.Size);
            Assert.AreEqual(0L, packet.SequenceNumber);
            Assert.IsTrue(packet.IsLastPacket);
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
                Assert.AreEqual(checksum, packet.Checksum);
                Assert.AreEqual(Packet.PacketSize, packet.Size);
                Assert.IsFalse(packet.IsLastPacket);
                packet.Read(reader, PacketFormatOption.ChecksumOnly, true);
                Assert.AreEqual(checksum2, packet.Checksum);
                Assert.AreEqual(5000, packet.Size);
                Assert.IsTrue(packet.IsLastPacket);
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
                Assert.AreEqual(checksum, reader.ReadUInt32());
                Assert.AreEqual(5000, reader.ReadInt32());
                Assert.AreEqual(true, reader.ReadBoolean());
                Assert.AreEqual(2L, reader.ReadInt64());
                byte[] readData = new byte[5000];
                Assert.AreEqual(5000, reader.Read(readData, 0, 5000));
                Assert.IsTrue(Utilities.CompareArray(data, 0, readData, 0, 5000));
                Assert.AreEqual(stream.Length, stream.Position);
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
                Assert.AreEqual(checksum, reader.ReadUInt32());
                byte[] readData = new byte[5000];
                Assert.AreEqual(5000, reader.Read(readData, 0, 5000));
                Assert.IsTrue(Utilities.CompareArray(data, 0, readData, 0, 5000));
                Assert.AreEqual(stream.Length, stream.Position);
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
                Assert.AreEqual(checksum, reader.ReadUInt32());
                Assert.AreEqual(5000, reader.ReadInt32());
                Assert.AreEqual(true, reader.ReadBoolean());
                byte[] readData = new byte[5000];
                Assert.AreEqual(5000, reader.Read(readData, 0, 5000));
                Assert.IsTrue(Utilities.CompareArray(data, 0, readData, 0, 5000));
                Assert.AreEqual(stream.Length, stream.Position);
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
                Assert.AreEqual(5000, reader.Read(readData, 0, 5000));
                Assert.IsTrue(Utilities.CompareArray(data, 0, readData, 0, 5000));
                Assert.AreEqual(stream.Length, stream.Position);
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
            Assert.IsTrue(Utilities.CompareArray(data, 0, readData, 0, 5000));
            packet.CopyTo(500, readData, 100, 1000);
            Assert.IsTrue(Utilities.CompareArray(data, 500, readData, 100, 1000));
        }

        [Test]
        public void TestEquals()
        {
            long checksum;
            byte[] data = GenerateData(Packet.PacketSize, out checksum);
            Packet packet1 = new Packet(data, Packet.PacketSize, 1, false);
            Packet packet2 = new Packet(data, Packet.PacketSize, 1, false);
            Assert.AreEqual(packet1, packet2);
            packet2 = new Packet(data, Packet.PacketSize, 1, true);
            Assert.AreNotEqual(packet1, packet2);
            packet2 = new Packet(data, Packet.PacketSize - 1, 1, true);
            Assert.AreNotEqual(packet1, packet2);
            byte[] data2 = GenerateData(Packet.PacketSize, out checksum);
            packet2 = new Packet(data2, Packet.PacketSize, 1, false);
            Assert.AreNotEqual(packet1, packet2);
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
}
