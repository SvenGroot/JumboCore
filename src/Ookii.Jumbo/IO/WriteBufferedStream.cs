// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Provides an extra layer of write buffering for a stream.
/// </summary>
/// <remarks>
/// <para>
///   This class is intended for use by the data servers when they send blocks to more efficiently
///   pack the data into packets than what <see cref="System.Net.Sockets.NetworkStream"/> provides.
/// </para>
/// <para>
///   Unfortunately, the <see cref="System.IO.BufferedStream"/> class did not provide the required
///   behaviour on Mono (it would bypass the buffer on a large write) so that's why this class is necessary.
/// </para>
/// </remarks>
public class WriteBufferedStream : Stream
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _bufferPos;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="WriteBufferedStream"/> class with the specified
    /// buffer size.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to write to.</param>
    /// <param name="bufferSize">The size of the buffer, in bytes.</param>
    public WriteBufferedStream(Stream stream, int bufferSize)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (bufferSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be larger than zero.");
        }

        if (!stream.CanWrite)
        {
            throw new ArgumentException("You must use a writable stream.", nameof(stream));
        }

        _stream = stream;
        _buffer = new byte[bufferSize];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WriteBufferedStream"/> class with the default buffer size of 64KB.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to write to.</param>
    public WriteBufferedStream(Stream stream)
        : this(stream, 65536)
    {
    }

    /// <summary>
    /// Gets a value that indicates whether the current stream supports reading.
    /// </summary>
    /// <value>
    /// Returns the value of <see cref="Stream.CanRead"/> of the underlying stream.
    /// </value>
    public override bool CanRead
    {
        get { return _stream.CanRead; }
    }

    /// <summary>
    /// Gets a value that indicates whether the current stream supports seeking.
    /// </summary>
    /// <value>
    /// Returns the value of <see cref="Stream.CanSeek"/> of the underlying stream.
    /// </value>
    public override bool CanSeek
    {
        get { return _stream.CanSeek; }
    }

    /// <summary>
    /// Gets a value that indicates whether the current stream supports writing.
    /// </summary>
    /// <value>
    /// Returns <see langword="true"/>.
    /// </value>
    public override bool CanWrite
    {
        get { return _stream.CanWrite; }
    }

    /// <summary>
    /// Flushes the contents of the write buffer to the underlying stream.
    /// </summary>
    public override void Flush()
    {
        CheckDisposed();
        if (_bufferPos > 0)
        {
            _stream.Write(_buffer, 0, _bufferPos);
        }

        _bufferPos = 0;
    }

    /// <summary>
    /// Gets the length of the stream.
    /// </summary>
    /// <value>
    /// The length of the underlying stream.
    /// </value>
    public override long Length
    {
        get
        {
            return _stream.Length + _bufferPos;
        }
    }

    /// <summary>
    /// Gets or sets the current stream position.
    /// </summary>
    /// <value>
    /// The current stream position.
    /// </value>
    public override long Position
    {
        get
        {
            return _stream.Position + _bufferPos;
        }
        set
        {
            Flush();
            _stream.Position = value;
        }
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
        Flush();
        return _stream.Read(buffer, offset, count);
    }

    /// <summary>
    /// Sets the position within the current stream.
    /// </summary>
    /// <param name="offset">A byte offset relative to the <paramref name="origin"/> parameter.</param>
    /// <param name="origin">A value of type <see cref="SeekOrigin"/> indicating the reference point used to obtain the new position.</param>
    /// <returns>The new position within the current stream.</returns>
    public override long Seek(long offset, SeekOrigin origin)
    {
        Flush();
        return _stream.Seek(offset, origin);
    }

    /// <summary>
    /// Sets the length of the current stream. 
    /// </summary>
    /// <param name="value">The desired length of the current stream in bytes.</param>
    public override void SetLength(long value)
    {
        Flush();
        _stream.SetLength(value);
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
        ArgumentNullException.ThrowIfNull(buffer);
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (offset + count > buffer.Length)
        {
            throw new ArgumentException("The sum of offset and count is greater than the buffer length.");
        }

        while (count > 0)
        {
            var length = Math.Min(_buffer.Length - _bufferPos, count);
            Array.Copy(buffer, offset, _buffer, _bufferPos, length);
            _bufferPos += length;
            count -= length;
            offset += length;
            if (_bufferPos == _buffer.Length)
            {
                Flush();
            }
        }
    }

    /// <summary>
    /// Releases the unmanaged resources used by the <see cref="WriteBufferedStream"/> and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
    /// <remarks>This function flushes the stream.</remarks>
    protected override void Dispose(bool disposing)
    {
        try
        {
            if (!_disposed)
            {
                Flush();
                if (disposing)
                {
                    _stream.Dispose();
                }

                _disposed = true;
            }
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

    private void CheckDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(this.GetType().FullName);
        }
    }
}
