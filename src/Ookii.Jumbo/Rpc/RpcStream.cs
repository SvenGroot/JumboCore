﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace Ookii.Jumbo.Rpc;

class RpcStream : Stream
{
    private readonly NetworkStream _baseStream;
    private readonly byte[] _buffer = new byte[0x1000]; // 4KB
    private int _dataLength;
    private int _dataOffset;
    private readonly byte[] _byteBuffer = new byte[256];

    public RpcStream(Socket socket)
    {
        ArgumentNullException.ThrowIfNull(socket);

        _baseStream = new NetworkStream(socket);
    }

    public RpcStream(TcpClient client)
    {
        ArgumentNullException.ThrowIfNull(client);

        _baseStream = client.GetStream();
    }

    public override bool CanRead
    {
        get { return true; }
    }

    public override bool CanSeek
    {
        get { return false; }
    }

    public override bool CanWrite
    {
        get { return true; }
    }

    public bool HasData
    {
        get { return _dataLength > 0; }
    }

    public void BeginBuffering(AsyncCallback callback)
    {
        _baseStream.BeginRead(_buffer, 0, _buffer.Length, callback, null);
    }

    public void EndBuffering(IAsyncResult ar)
    {
        _dataOffset = 0;
        _dataLength = _baseStream.EndRead(ar);
    }

    public int FillBuffer()
    {
        var count = _baseStream.Read(_buffer, 0, _buffer.Length);
        _dataOffset = 0;
        _dataLength = count;
        return count;
    }

    public override void Flush()
    {
    }

    public string ReadString()
    {
        var length = ReadByte();
        if (length == 0)
        {
            return string.Empty;
        }

        Read(_byteBuffer, 0, length);
        return Encoding.UTF8.GetString(_byteBuffer, 0, length);
    }

    public override long Length
    {
        get { throw new NotSupportedException(); }
    }

    public override long Position
    {
        get
        {
            throw new NotSupportedException();
        }
        set
        {
            throw new NotSupportedException();
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesRead = 0;
        if (_dataLength > 0)
        {
            var realCount = Math.Min(_dataLength, count);
            Buffer.BlockCopy(_buffer, _dataOffset, buffer, offset, realCount);
            _dataLength -= realCount;
            _dataOffset += realCount;
            offset += realCount;
            count -= realCount;
            bytesRead += realCount;
        }
        while (count > 0)
        {
            if (FillBuffer() == 0)
            {
                throw new RpcException("Remote socket was closed.");
            }

            var realCount = Math.Min(_dataLength, count);
            Buffer.BlockCopy(_buffer, _dataOffset, buffer, offset, realCount);
            _dataLength -= realCount;
            _dataOffset += realCount;
            offset += realCount;
            count -= realCount;
            bytesRead += realCount;
        }
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _baseStream.Write(buffer, 0, count);
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            _baseStream.Dispose();
        }
    }
}
