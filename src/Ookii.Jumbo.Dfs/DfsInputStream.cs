// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

#pragma warning disable SYSLIB0011 // BinaryFormatter is deprecated.

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides a stream for reading a block from the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsInputStream : Stream, IRecordInputStream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DfsInputStream));

        private readonly INameServerClientProtocol _nameServer;
        private readonly JumboFile _file;
        private long _position;
        private bool _disposed;
        private readonly Packet _currentPacket = new Packet();
        private int _currentPacketOffset = 0;
        private long _endOffset;
        private int _lastBlockToDownload;
        private long _paddingSkipped;

        private TcpClient? _serverClient;
        private NetworkStream? _serverStream;
        private BinaryReader? _serverReader;
        private List<ServerAddress>? _dataServers;
        private int _currentServerIndex = 0;
        private Guid _currentBlockId;


        /// <summary>
        /// Initializes a new instance of the <see cref="DfsInputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to read.</param>
        public DfsInputStream(INameServerClientProtocol nameServer, string path)
        {
            ArgumentNullException.ThrowIfNull(nameServer);
            ArgumentNullException.ThrowIfNull(path);

            _nameServer = nameServer;
            _log.DebugFormat("Opening file {0} from the DFS.", path);
            _file = nameServer.GetFileInfo(path);
            // GetFileInfo doesn't throw if the file doesn't exist; we do.
            if (_file == null)
                throw new FileNotFoundException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The file '{0}' does not exist on the distributed file system.", path));
            BlockSize = (int)_file.BlockSize;
            _endOffset = _file.Size;
        }

        /// <summary>
        /// Ensures that resources are freed and other cleanup operations are performed when the garbage collector reclaims the <see cref="DfsInputStream"/>.
        /// </summary>
        ~DfsInputStream()
        {
            Dispose(false);
        }

        /// <summary>
        /// Gets the size of the blocks of this file.
        /// </summary>
        /// <value>
        /// The size of the blocks of this file, in bytes.
        /// </value>
        /// <remarks>
        /// This value doesn't need to be the same as the default block size specified by <see cref="NameServerConfigurationElement.BlockSize"/>
        /// </remarks>
        public int BlockSize { get; private set; }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports reading.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanRead
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports seeking.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanSeek
        {
            get { return true; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports writing.
        /// </summary>
        /// <value>
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanWrite
        {
            get { return false; }
        }

        /// <summary>
        /// This method is not used for this class; it does nothing.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        /// <value>
        /// The size of the file in the distributed file system.
        /// </value>
        public override long Length
        {
            get
            {
                return _file.Size;
            }
        }

        /// <summary>
        /// Gets or sets the current stream position.
        /// </summary>
        /// <returns>The current position within the stream.</returns>
        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                if (value < 0 || value >= Length)
                    throw new ArgumentOutOfRangeException(nameof(value));

                Seek(value, SeekOrigin.Begin);
            }
        }

        /// <summary>
        /// Gets the number of errors encountered while reading data from the data servers.
        /// </summary>
        /// <value>
        /// The number of errors encountered while reading data from the data servers.
        /// </value>
        /// <remarks>
        /// If the read operation completes successfully, and this value is higher than zero, it means the error was
        /// recovered from.
        /// </remarks>
        public int DataServerErrors { get; private set; }

        /// <summary>
        /// Gets the number of blocks read.
        /// </summary>
        /// <value>
        /// The number of blocks read.
        /// </value>
        public int BlocksRead { get; private set; }

        /// <summary>
        /// Gets the record options applied to this stream.
        /// </summary>
        /// <value>
        /// One or more of the <see cref="RecordStreamOptions" /> values indicating the record options applied to this stream.
        /// </value>
        public RecordStreamOptions RecordOptions
        {
            get { return _file.RecordOptions; }
        }


        /// <summary>
        /// Gets or sets the position in the stream after which no data will be read.
        /// </summary>
        /// <value>
        /// The position after which <see cref="System.IO.Stream.Read(Byte[], int, int)"/> method will not return any data. The default value is the length of the stream.
        /// </value>
        /// <remarks>
        /// 	<para>
        /// For a stream where <see cref="RecordOptions"/> is set to <see cref="RecordStreamOptions.DoNotCrossBoundary"/> you can use this property
        /// to ensure that no data after the boundary is read if you only wish to read records up to the boundary.
        /// </para>
        /// 	<para>
        /// On the Jumbo DFS, crossing a block boundary will cause a network connection to be established and data to be read from
        /// a different data server. If you are reading records from only a single block (as is often the case for Jumbo Jet tasks)
        /// this property can be used to ensure that no data from the next block will be read.
        /// </para>
        /// 	<para>
        /// Setting this property to a value other than the stream length if <see cref="RecordStreamOptions.DoNotCrossBoundary"/> is not set, or
        /// to a value that is not on a structural boundary can cause reading to halt in the middle of a record, and is therefore not recommended.
        /// </para>
        /// </remarks>
        public long StopReadingAtPosition
        {
            get { return _endOffset; }
            set
            {
                if (value < _position || value > Length)
                    throw new ArgumentOutOfRangeException(nameof(value));

                _endOffset = value;
                if (value > 0)
                    _lastBlockToDownload = (int)((value - 1) / _file.BlockSize);
                else
                    _lastBlockToDownload = 0;
            }
        }

        /// <summary>
        /// Gets the amount of padding skipped while reading from the stream.
        /// </summary>
        /// <value>The amount of padding bytes skipped.</value>
        public long PaddingBytesSkipped
        {
            get { return _paddingSkipped; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has stopped reading.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the stream has reached the position indicated by <see cref="StopReadingAtPosition"/>; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// If this property is <see langword="true"/> it means the next call to <see cref="Read"/> will return 0.
        /// </remarks>
        public bool IsStopped
        {
            get { return _position >= _endOffset; }
        }

        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read. 
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between offset and (offset + count - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_disposed)
                throw new ObjectDisposedException(typeof(DfsInputStream).FullName);
            // These exceptions match the contract given in the Stream class documentation.
            ArgumentNullException.ThrowIfNull(buffer);
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (offset + count > buffer.Length)
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if (_position + count > _endOffset)
                count = (int)(_endOffset - _position);

            var sizeRemaining = count;
            if (count > 0)
            {
                while (_position < _endOffset && sizeRemaining > 0)
                {
                    if (_currentPacketOffset >= _currentPacket.Size)
                    {
                        _currentPacketOffset = 0;
                        // ReadPacket returns false if the end of the file or StopReadingAtPosition has been reached.
                        if (!ReadPacket())
                            break;
                    }

                    var packetCount = Math.Min(_currentPacket.Size - _currentPacketOffset, sizeRemaining);

                    var copied = _currentPacket.CopyTo(_currentPacketOffset, buffer, offset, packetCount);
                    Debug.Assert(copied == packetCount);
                    offset += copied;
                    sizeRemaining -= copied;
                    _position += copied;
                    _currentPacketOffset += copied;

                    if (_currentPacket.IsLastPacket && _position < Length && _currentPacketOffset == _currentPacket.Size && _currentPacket.Size < Packet.PacketSize)
                    {
                        var padding = Packet.PacketSize - _currentPacket.Size;
                        _position += padding;
                        _paddingSkipped += padding;
                    }
                }
            }
            return count - sizeRemaining;
        }

        /// <summary>
        /// Sets the position within the current stream.
        /// </summary>
        /// <param name="offset">A byte offset relative to the <paramref name="origin"/> parameter.</param>
        /// <param name="origin">A value of type <see cref="SeekOrigin"/> indicating the reference point used to obtain the new position.</param>
        /// <returns>The new position within the current stream.</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition = 0;
            switch (origin)
            {
            case SeekOrigin.Begin:
                newPosition = offset;
                break;
            case SeekOrigin.Current:
                newPosition = _position + offset;
                break;
            case SeekOrigin.End:
                newPosition = Length + offset;
                break;
            }
            if (newPosition < 0 || newPosition >= Length)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (newPosition != _position)
            {
                CloseDataServerConnection();
                _position = newPosition;
                _currentPacketOffset = (int)(_position % Packet.PacketSize);
                _currentPacket.Clear();
            }
            return _position;
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="value">The desired length of the current stream in bytes.</param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="buffer">An array of bytes. This method copies <paramref name="count" /> bytes from <paramref name="buffer" /> to the current stream.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin copying bytes to the current stream.</param>
        /// <param name="count">The number of bytes to be written to the current stream.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Determines the offset of the specified position from the directly preceding structural boundary (e.g. a block boundary on the DFS).
        /// </summary>
        /// <param name="position">The position.</param>
        /// <returns>
        /// The offset from the structural boundary that directly precedes the specified position.
        /// </returns>
        public long OffsetFromBoundary(long position)
        {
            if (position < 0 || position > Length)
                throw new ArgumentOutOfRangeException(nameof(position));

            return position % _file.BlockSize;
        }

        /// <summary>
        /// Determines whether the range between two specified positions does not cross a structural boundary (e.g. a block boundary on the DFS).
        /// </summary>
        /// <param name="position1">The first position.</param>
        /// <param name="position2">The second position.</param>
        /// <returns>
        /// 	<see langword="true"/> if the <paramref name="position1"/> and <paramref name="position2"/> fall inside the same boundaries (e.g. if
        /// both positions are in the same block in the DFS); otherwise, <see langword="false"/>.
        /// </returns>
        public bool AreInsideSameBoundary(long position1, long position2)
        {
            if (position1 < 0 || position1 > Length)
                throw new ArgumentOutOfRangeException(nameof(position1));
            if (position2 < 0 || position2 > Length)
                throw new ArgumentOutOfRangeException(nameof(position2));

            return position1 / _file.BlockSize == position2 / _file.BlockSize;
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="DfsInputStream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (!_disposed)
            {
                _disposed = true;
                if (disposing)
                {
                    CloseDataServerConnection();
                }
                //if( _readTime != null )
                //    _log.DebugFormat("Total: {0}, count: {1}, average: {2}", _readTime.ElapsedMilliseconds, _totalReads, _readTime.ElapsedMilliseconds / (float)_totalReads);
            }
        }

        private bool ReadPacket()
        {
            var success = false;
            do
            {
                try
                {
                    if (_serverClient == null)
                    {
                        if (!ConnectToDataServer())
                            return false;
                    }

                    var status = (DataServerClientProtocolResult)_serverReader!.ReadInt16();
                    if (status != DataServerClientProtocolResult.Ok)
                    {
                        throw new DfsException("The data server reported an error.");
                    }
                    _currentPacket.Read(_serverReader, PacketFormatOption.NoSequenceNumber, true);
                    if (_currentPacket.IsLastPacket)
                    {
                        CloseDataServerConnection();
                        _currentServerIndex = 0;
                    }
                    success = true;
                }
                catch (Exception ex)
                {
                    _log.Error(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Error reading block {0} from server {1}", _currentBlockId, _dataServers![_currentServerIndex]), ex);
                    CloseDataServerConnection();
                    ++_currentServerIndex;
                    DataServerErrors++;
                    if (_currentServerIndex == _dataServers.Count)
                        throw;
                }
            } while (!success);
            return true;
        }

        private bool ConnectToDataServer()
        {
            if (_position == _file.Size)
                return false;

            var success = false;
            ++BlocksRead;
            do
            {
                CloseDataServerConnection();
                var blockIndex = (int)(_position / _file.BlockSize);
                if (_lastBlockToDownload > 0 && blockIndex > _lastBlockToDownload)
                    return false;
                var blockOffset = (int)(_position % _file.BlockSize);
                var blockId = _file.Blocks[blockIndex];
                _currentBlockId = blockId;
                _dataServers = _nameServer.GetDataServersForBlock(blockId).ToList();
                var server = _dataServers[_currentServerIndex];
                _serverClient = new TcpClient(server.HostName, server.Port);
                _serverStream = _serverClient.GetStream();
                _serverReader = new BinaryReader(_serverStream);
                var header = new DataServerClientProtocolReadHeader()
                {
                    BlockId = blockId,
                    Offset = blockOffset,
                    Size = -1
                };

                var formatter = new BinaryFormatter();
                formatter.Serialize(_serverStream, header);
                _serverStream.Flush();

                var status = (DataServerClientProtocolResult)_serverReader.ReadInt16();
                if (status == DataServerClientProtocolResult.OutOfRange && blockOffset > 0 && blockIndex < _file.Blocks.Count - 1 && _file.RecordOptions == IO.RecordStreamOptions.DoNotCrossBoundary)
                {
                    var oldPosition = _position;
                    _position = (blockIndex + 1) * _file.BlockSize;
                    _paddingSkipped += _position - oldPosition;
                }
                else if (status != DataServerClientProtocolResult.Ok)
                {
                    ++_currentServerIndex;
                    if (_currentServerIndex == _dataServers.Count)
                        throw new DfsException("The server encountered an error while sending data.");
                }
                else
                {
                    blockOffset = _serverReader.ReadInt32();
                    _currentPacketOffset = (int)(_position % Packet.PacketSize);
                    success = true;
                }
            } while (!success);
            return true;
        }

        private void CloseDataServerConnection()
        {
            if (_serverReader != null)
            {
                _serverReader.Dispose();
                _serverReader = null;
            }
            if (_serverStream != null)
            {
                _serverStream.Dispose();
                _serverStream = null;
            }
            if (_serverClient != null)
            {
                _serverClient.Close();
                _serverClient = null;
            }
        }
    }
}
