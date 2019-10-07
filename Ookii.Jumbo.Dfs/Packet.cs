// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents a part of a block.
    /// </summary>
    /// <remarks>
    /// Packets are the unit in which block data is stored and transferred over the network. Checksums are
    /// computed and stored on a per-packet basis, and each write or read request to a data server must always
    /// involve a whole number of packets. All packets except the last must equal <see cref="PacketSize"/>.
    /// </remarks>
    public class Packet
    {
        /// <summary>
        /// The size of a single packet.
        /// </summary>
        public const int PacketSize = 0x10000; // TODO: This should probably be a parameter of the file system.

        private static readonly bool _computeChecksums = DfsConfiguration.GetConfiguration().Checksum.IsEnabled;
        private readonly byte[] _data = new byte[PacketSize];
        private readonly Crc32Checksum _checksum = new Crc32Checksum();
        private long _checksumValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="Packet"/> class with no data.
        /// </summary>
        public Packet()
        {
        }

        /// <summary>
        /// Initailizes a new instance of the <see cref="Packet"/> class with the specified data.
        /// </summary>
        /// <param name="data">The data to store in the packet.</param>
        /// <param name="size">The size of the data to store in the packet.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="isLastPacket"><see langword="true"/> if this is the last packet being sent; otherwise <see langword="false"/>.</param>
        public Packet(byte[] data, int size, long sequenceNumber, bool isLastPacket)
        {
            CopyFrom(data, size, sequenceNumber, isLastPacket);
        }

        /// <summary>
        /// Gets or sets a value that indicates whether this packet is the last packet being sent.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if this packs is the last packet being sent; otherwise, <see langword="false" />.
        /// </value>
        public bool IsLastPacket { get; set; }

        /// <summary>
        /// Gets or sets the size of the packet.
        /// </summary>
        /// <value>
        /// The size of the packet, in bytes.
        /// </value>
        /// <remarks>
        /// This value will always be less than or equal to <see cref="PacketSize" />. If
        /// <see cref="IsLastPacket" /> is <see langword="false" />, it will be equal to
        /// <see cref="PacketSize" />.
        /// </remarks>
        public int Size { get; private set; }

        /// <summary>
        /// Gets or sets the sequence number of the packet.
        /// </summary>
        /// <value>
        /// The sequence number.
        /// </value>
        public long SequenceNumber { get; set; }

        /// <summary>
        /// Gets or sets the checksum for the data in this packet.
        /// </summary>
        /// <value>
        /// The checksum for the data in this packet, or 0 if checksums are disabled.
        /// </value>
        public long Checksum
        {
            get
            {
                return _checksumValue;
            }
        }

        /// <summary>
        /// Resets the data in the packet using the specified data.
        /// </summary>
        /// <param name="data">The data to store in the packet.</param>
        /// <param name="size">The size of the data to store in the packet.</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="isLastPacket"><see langword="true"/> if this is the last packet being sent; otherwise <see langword="false"/>.</param>
        public void CopyFrom(byte[] data, int size, long sequenceNumber, bool isLastPacket)
        {
            if( data == null )
                throw new ArgumentNullException(nameof(data));
            if( size < 0 || size > data.Length || size > PacketSize )
                throw new ArgumentOutOfRangeException(nameof(size));
            if( !isLastPacket && size != PacketSize )
                throw new ArgumentException("The packet has an invalid size.");

            Array.Copy(data, _data, size);
            Size = size;
            IsLastPacket = isLastPacket;
            SequenceNumber = sequenceNumber;
            RecomputeChecksum();
        }

        /// <summary>
        /// Resets the data in the packet using the specified stream.
        /// </summary>
        /// <param name="stream">The stream containing the data..</param>
        /// <param name="sequenceNumber">The sequence number.</param>
        /// <param name="isLastPacket"><see langword="true"/> if this is the last packet being sent; otherwise <see langword="false"/>.</param>
        public void CopyFrom(Stream stream, long sequenceNumber, bool isLastPacket)
        {
            if( stream == null )
                throw new ArgumentNullException(nameof(stream));

            int size = (int)Math.Min(PacketSize, stream.Length);
            if( !isLastPacket && size != PacketSize )
                throw new ArgumentException("The packet has an invalid size.");

            stream.Read(_data, 0, size);
            Size = size;
            IsLastPacket = isLastPacket;
            SequenceNumber = sequenceNumber;
            RecomputeChecksum();
        }

        /// <summary>
        /// Resets the data in the packet using the data from the specified packet.
        /// </summary>
        /// <param name="packet">The packet whose data to copy.</param>
        public void CopyFrom(Packet packet)
        {
            if( packet == null )
                throw new ArgumentNullException(nameof(packet));

            Array.Copy(packet._data, _data, packet.Size);
            Size = packet.Size;
            IsLastPacket = packet.IsLastPacket;
            SequenceNumber = packet.SequenceNumber;
            _checksumValue = packet.Checksum;
        }

        /// <summary>
        /// Copies the packet's data to the specified buffer.
        /// </summary>
        /// <param name="sourceOffset">The offset in the packet to start copying the data from.</param>
        /// <param name="buffer">The buffer to copy the data to.</param>
        /// <param name="destinationOffset">The offset in <paramref name="buffer"/> to start writing the data to.</param>
        /// <param name="count">The maximum number of bytes to copy into the buffer.</param>
        /// <returns>The actual number of bytes written into the buffer.</returns>
        public int CopyTo(int sourceOffset, byte[] buffer, int destinationOffset, int count)
        {
            if( buffer == null )
                throw new ArgumentNullException(nameof(buffer));
            if( sourceOffset < 0 || sourceOffset >= Size )
                throw new ArgumentOutOfRangeException(nameof(sourceOffset));
            if( destinationOffset < 0 )
                throw new ArgumentOutOfRangeException(nameof(destinationOffset));
            if( count < 0 )
                throw new ArgumentOutOfRangeException(nameof(count));
            if( destinationOffset + count > buffer.Length )
                throw new ArgumentException("The combined value of destOffset and count is larger than the buffer size.");

            count = Math.Min(count, Size - sourceOffset);
            Array.Copy(_data, sourceOffset, buffer, destinationOffset, count);
            return count;
        }

        /// <summary>
        /// Reads packet data from a <see cref="BinaryReader"/>.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to read the packe data from.</param>
        /// <param name="format">The format.</param>
        /// <param name="verifyChecksum"><see langword="true"/> to verify the checksum read from the data source against
        /// the actual checksum of the data; <see langword="false"/> to skip verifying the checksum.</param>
        public void Read(BinaryReader reader, PacketFormatOption format, bool verifyChecksum)
        {
            if( reader == null )
                throw new ArgumentNullException(nameof(reader));

            uint expectedChecksum = reader.ReadUInt32();
            if( format == PacketFormatOption.ChecksumOnly )
            {
                // Determine the size from the stream length.
                Size = (int)Math.Min(reader.BaseStream.Length - reader.BaseStream.Position, PacketSize);
                IsLastPacket = reader.BaseStream.Length - reader.BaseStream.Position <= PacketSize;
            }
            else
            {
                Size = reader.ReadInt32();
                IsLastPacket = reader.ReadBoolean();
                if( format != PacketFormatOption.NoSequenceNumber )
                    SequenceNumber = reader.ReadInt64();
                if( Size > PacketSize || (!IsLastPacket && Size != PacketSize) )
                    throw new InvalidPacketException("The packet has an invalid size.");
            }
            int bytesRead = 0;
            // We loop because the reader may use a NetworkStream which might not return all data at once.
            while( bytesRead < Size )
            {
                bytesRead += reader.Read(_data, bytesRead, Size - bytesRead);
            }

            if( _computeChecksums && verifyChecksum )
            {
                RecomputeChecksum();
                if( Checksum != expectedChecksum )
                {
                    throw new InvalidPacketException("Computed packet checksum doesn't match expected checksum.");
                }
            }
            else
                _checksumValue = expectedChecksum;
        }

        /// <summary>
        /// Writes the packet to the specified <see cref="BinaryWriter"/>.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the packet to.</param>
        /// <param name="format">The format.</param>
        public void Write(BinaryWriter writer, PacketFormatOption format)
        {
            if( writer == null )
                throw new ArgumentNullException(nameof(writer));

            writer.Write((uint)Checksum);
            if( format != PacketFormatOption.ChecksumOnly )
            {
                writer.Write(Size);
                writer.Write(IsLastPacket);
                if( format != PacketFormatOption.NoSequenceNumber )
                    writer.Write(SequenceNumber);
            }
            writer.Write(_data, 0, Size);
        }

        /// <summary>
        /// Writes the packet data, without any header, to the specified <see cref="Stream"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write the packet data to.</param>
        public void WriteDataOnly(Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException(nameof(stream));

            stream.Write(_data, 0, Size);
        }

        /// <summary>
        /// Compares this <see cref="Packet"/> with another object.
        /// </summary>
        /// <param name="obj">The object to compare with.</param>
        /// <returns><see langword="true"/> if the object equals this instance; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            Packet other = obj as Packet;
            if( other != null )
            {
                if( IsLastPacket == other.IsLastPacket && Size == other.Size && Checksum == other.Checksum && SequenceNumber == other.SequenceNumber )
                {
                    for( int x = 0; x < Size; ++x )
                    {
                        if( _data[x] != other._data[x] )
                            return false;
                    }
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets a hash code for this object.
        /// </summary>
        /// <returns>A hash code for this packet.</returns>
        /// <remarks>
        /// No factual implementation of this method is prevented, the method is only overridden to prevent the
        /// compiler warning against overriding <see cref="Equals(Object)"/> but not <see cref="GetHashCode"/>.
        /// </remarks>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        private void RecomputeChecksum()
        {
            if( _computeChecksums )
            {
                _checksum.Reset();
                _checksum.Update(_data, 0, Size);
                _checksumValue = _checksum.Value;
            }
        }

        /// <summary>
        /// Clears this instance.
        /// </summary>
        public void Clear()
        {
            Size = 0;
            _checksum.Reset();
            _checksumValue = 0;
        }
    }
}
