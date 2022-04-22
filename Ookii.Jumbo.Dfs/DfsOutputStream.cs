// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides a stream for writing files to the distributed file system.
    /// </summary>
    /// <threadsafety static="true" instance="false" />
    public class DfsOutputStream : Stream, IRecordOutputStream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DfsOutputStream));
        private readonly Packet _packet = new Packet();
        private long _nextSequenceNumber;
        private BlockSender _sender;
        private BlockAssignment _block;
        private readonly INameServerClientProtocol _nameServer;
        private readonly string _path;
        private readonly RecordStreamOptions _recordOptions;
        private readonly bool _useLocalReplica;
        private int _blockBytesWritten;
        private readonly byte[] _buffer = new byte[Packet.PacketSize];
        private readonly MemoryStream _recordBuffer;
        private int _bufferPos;
        private bool _disposed = false;
        private long _fileBytesWritten;
        private long _length;
        private long _padding;

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to write.</param>
        public DfsOutputStream(INameServerClientProtocol nameServer, string path)
            : this(nameServer, path, 0, 0, true, RecordStreamOptions.None)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutputStream"/> class.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to write.</param>
        /// <param name="recordOptions">The record options for the file.</param>
        public DfsOutputStream(INameServerClientProtocol nameServer, string path, RecordStreamOptions recordOptions)
            : this(nameServer, path, 0, 0, true, recordOptions)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to write.</param>
        /// <param name="blockSize">The size of the blocks of the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        public DfsOutputStream(INameServerClientProtocol nameServer, string path, int blockSize, int replicationFactor)
            : this(nameServer, path, blockSize, replicationFactor, true, RecordStreamOptions.None)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsOutputStream"/> with the specified name server and file.
        /// </summary>
        /// <param name="nameServer">The <see cref="INameServerClientProtocol"/> interface of the name server for the distributed
        /// file system.</param>
        /// <param name="path">The path of the file to write.</param>
        /// <param name="blockSize">The size of the blocks of the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="recordOptions">The record options for the file.</param>
        public DfsOutputStream(INameServerClientProtocol nameServer, string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            if (nameServer == null)
                throw new ArgumentNullException(nameof(nameServer));
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            if (blockSize < 0)
                throw new ArgumentOutOfRangeException(nameof(blockSize), "Block size must be zero or greater.");
            if (blockSize % Packet.PacketSize != 0)
                throw new ArgumentException("Block size must be a multiple of the packet size.", nameof(blockSize));
            if (replicationFactor < 0)
                throw new ArgumentOutOfRangeException(nameof(replicationFactor), "Replication factor must be zero or greater.");

            if (blockSize == 0)
            {
                BlockSize = nameServer.BlockSize;
            }
            else
                BlockSize = blockSize;
            _nameServer = nameServer;
            _path = path;
            _recordOptions = recordOptions;
            _useLocalReplica = useLocalReplica;
            _log.DebugFormat("Creating file {0} on name server.", _path);
            _block = nameServer.CreateFile(path, blockSize, replicationFactor, useLocalReplica, recordOptions);
            if ((recordOptions & RecordStreamOptions.DoNotCrossBoundary) == RecordStreamOptions.DoNotCrossBoundary)
            {
                _recordBuffer = new MemoryStream(Packet.PacketSize);
            }
        }

        /// <summary>
        /// Finalizes this instance of the <see cref="DfsOutputStream"/> class.
        /// </summary>
        ~DfsOutputStream()
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
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanRead
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports seeking.
        /// </summary>
        /// <value>
        /// Returns <see langword="false"/>.
        /// </value>
        public override bool CanSeek
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value that indicates whether the current stream supports writing.
        /// </summary>
        /// <value>
        /// Returns <see langword="true"/>.
        /// </value>
        public override bool CanWrite
        {
            get { return true; }
        }

        /// <summary>
        /// This method is not used; it does nothing.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Gets the length of the stream.
        /// </summary>
        /// <value>
        /// The total number of bytes written to the stream so far.
        /// </value>
        public override long Length
        {
            get { return _length; }
        }

        /// <summary>
        /// Gets the current stream position.
        /// </summary>
        /// <value>
        /// The current stream position. This value is always equal to <see cref="Length"/>.
        /// </value>
        /// <remarks>
        /// Setting this property is not supported and throws an exception.
        /// </remarks>
        public override long Position
        {
            get
            {
                return _length;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Gets the options applied to records in the stream.
        /// </summary>
        /// <value>One or more of the <see cref="RecordStreamOptions"/> values.</value>
        public RecordStreamOptions RecordOptions
        {
            get { return _recordOptions; }
        }

        /// <summary>
        /// Gets the amount of the stream that is actually used by records.
        /// </summary>
        /// <value>The length of the stream minus padding.</value>
        public long RecordsSize
        {
            get { return _length - _padding; }
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between <paramref name="offset" /> and (<paramref name="offset" /> + <paramref name="count" /> - 1) replaced by the bytes read from the current source.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin storing the data read from the current stream.</param>
        /// <param name="count">The maximum number of bytes to be read from the current stream.</param>
        /// <returns>
        /// The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.
        /// </returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Not supported.
        /// </summary>
        /// <param name="offset">A byte offset relative to the <paramref name="origin" /> parameter.</param>
        /// <param name="origin">A value of type <see cref="T:System.IO.SeekOrigin" /> indicating the reference point used to obtain the new position.</param>
        /// <returns>
        /// The new position within the current stream.
        /// </returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
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
        /// Writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
        /// </summary>
        /// <param name="buffer">An array of bytes. This method copies count bytes from buffer to the current stream.</param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin copying bytes to the current stream.</param>
        /// <param name="count">The number of bytes to be written to the current stream.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();
            // These exceptions match the contract given in the Stream class documentation.
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (offset + count > buffer.Length)
                throw new ArgumentException("The sum of offset and count is greater than the buffer length.");

            if (_sender != null)
                _sender.ThrowIfErrorOccurred();

            // If the DoNotCrossBoundary option is set, we write records into a temporary buffer and do not add them to the real buffer until MarkRecord is called.
            if (_recordBuffer != null)
            {
                _recordBuffer.Write(buffer, offset, count);
                _length += count;
            }
            else
            {
                var bufferPos = offset;
                var end = offset + count;

                while (bufferPos < end)
                {
                    if (_bufferPos == _buffer.Length)
                    {
                        WriteBufferToPacket(false);
                    }
                    var bufferRemaining = _buffer.Length - _bufferPos;
                    var writeSize = Math.Min(end - bufferPos, bufferRemaining);
                    Array.Copy(buffer, bufferPos, _buffer, _bufferPos, writeSize);
                    _bufferPos += writeSize;
                    bufferPos += writeSize;
                    _length += writeSize;
                    System.Diagnostics.Debug.Assert(_bufferPos <= _buffer.Length);
                }
            }
        }

        /// <summary>
        /// Indicates that the current position of the stream is a record boundary.
        /// </summary>
        public void MarkRecord()
        {
            // _recordBuffer not null means DoNotCrossBoundary option is set.
            if (_recordBuffer != null && _recordBuffer.Length > 0)
            {
                if (_recordBuffer.Length > BlockSize)
                    throw new InvalidOperationException("The record is larger than a block."); // TODO: Allow this.

                // Does the record fit in the current block?
                if (_blockBytesWritten + _bufferPos + _recordBuffer.Length > BlockSize)
                {
                    var padding = BlockSize - (_blockBytesWritten + _bufferPos);
                    // Write the current buffer contents to the block and start a new block. We do this even if _bufferPos == 0 because we at least have to tell the block server the block is finished.
                    WriteBufferToPacket(true);
                    // Correct length to account for padding added to the block.
                    _length += padding;
                    _padding += padding;
                }

                _recordBuffer.Position = 0;
                var remaining = (int)_recordBuffer.Length;
                // If there is data in the buffer, we copy this record into the current buffer.
                if (_bufferPos > 0)
                {
                    if (_bufferPos < _buffer.Length)
                    {
                        var bytesRead = _recordBuffer.Read(_buffer, _bufferPos, _buffer.Length - _bufferPos);
                        remaining -= bytesRead;
                        _bufferPos += bytesRead;
                    }

                    if (_bufferPos == _buffer.Length)
                        WriteBufferToPacket(false);
                }

                // If the data left in the record is bigger than a single packet, we add those whole packets to the sender.
                while (remaining > Packet.PacketSize)
                {
                    // We can never cross a record boundary here because of the check above, so no need to check for final packets.
                    WritePacket(_recordBuffer, false);
                    _blockBytesWritten += Packet.PacketSize;
                    _fileBytesWritten += Packet.PacketSize;
                    remaining -= Packet.PacketSize;
                }

                // And copy the remaining data into the buffer.
                if (remaining > 0)
                {
                    // _bufferPos should be zero here but anyway.
                    _bufferPos += _recordBuffer.Read(_buffer, _bufferPos, remaining);
                }

                _recordBuffer.SetLength(0);
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="DfsOutputStream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        /// <remarks>
        /// This function writes all remaining data the data server, waits until sending the packets is finished, and closes
        /// the file on the name server.
        /// </remarks>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (!_disposed)
            {
                try
                {
                    _disposed = true;
                    if (_bufferPos > 0 || _fileBytesWritten == 0 && _block != null)
                    {
                        WritePacket(_buffer, _bufferPos, true);
                        _bufferPos = 0;
                    }
                    try
                    {
                        if (_sender != null)
                        {
                            _sender.WaitForAcknowledgements();
                        }
                    }
                    finally
                    {
                        if (disposing)
                        {
                            if (_sender != null)
                                _sender.Dispose();
                            if (_recordBuffer != null)
                                _recordBuffer.Dispose();
                        }
                    }
                }
                finally
                {
                    _nameServer.CloseFile(_path);
                }
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(typeof(DfsOutputStream).FullName);
        }

        private void WritePacket(byte[] buffer, int length, bool finalPacket)
        {
            EnsureSenderCreated();
            _packet.CopyFrom(buffer, length, _nextSequenceNumber++, finalPacket);
            _sender.SendPacket(_packet);
        }

        private void EnsureSenderCreated()
        {
            if (_sender == null)
            {
                if (_block == null)
                    _block = _nameServer.AppendBlock(_path, _useLocalReplica);
                _sender = new BlockSender(_block);
            }
        }

        private void WritePacket(Stream stream, bool finalPacket)
        {
            EnsureSenderCreated();
            _packet.CopyFrom(stream, _nextSequenceNumber++, finalPacket);
            _sender.SendPacket(_packet);
        }

        private void WriteBufferToPacket(bool forceFinalPacket)
        {
            System.Diagnostics.Debug.Assert(_blockBytesWritten + _bufferPos <= BlockSize);
            var finalPacket = forceFinalPacket || _blockBytesWritten + _bufferPos == BlockSize;
            WritePacket(_buffer, _bufferPos, finalPacket);
            _blockBytesWritten += _bufferPos;
            _fileBytesWritten += _bufferPos;
            _bufferPos = 0;
            if (finalPacket)
            {
                // Do we really want to wait here? We could just let it run in the background and continue on our
                // merry way. That would require keeping track of them so we know in Dispose when we're really finished.
                // It would also require the name server to allow appending of new blocks while old ones are still pending.
                _sender.WaitForAcknowledgements();
                _sender.ThrowIfErrorOccurred();
                _sender = null;
                _block = null;
                _blockBytesWritten = 0;
            }
        }
    }
}
