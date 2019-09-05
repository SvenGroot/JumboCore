// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    class ChecksumOutputStream : Stream
    {
        private readonly Stream _baseStream;
        private readonly bool _ownsBaseStream;
        private readonly Crc32 _checksum;
        private long _bytesWritten;
        private bool _disposed;

        public ChecksumOutputStream(Stream baseStream, bool ownsBaseStream, bool enableChecksum)
        {
            if( baseStream == null )
                throw new ArgumentNullException("baseStream");

            _baseStream = baseStream;
            _ownsBaseStream = ownsBaseStream;

            if( enableChecksum )
            {
                _checksum = new Crc32();
            }
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
            _baseStream.Flush();
        }

        public override long Length
        {
            get { return _bytesWritten; }
        }

        public override long Position
        {
            get
            {
                return _bytesWritten;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
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
            if( _disposed )
                throw new ObjectDisposedException(typeof(ChecksumOutputStream).FullName);

            if( count == 0 )
                return;

            if( _bytesWritten == 0 )
                _baseStream.WriteByte((byte)((_checksum != null) ? 1 : 0));

            _baseStream.Write(buffer, offset, count);
            _bytesWritten += count;

            if( _checksum != null )
                _checksum.Update(buffer, offset, count);
        }

        private void FinalizeChecksum()
        {
            if( _checksum != null && _bytesWritten > 0 )
            {
                byte[] sum = BitConverter.GetBytes(_checksum.ValueUInt32);
                _baseStream.Write(sum, 0, sum.Length);
            }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed )
                {
                    _disposed = true;
                    FinalizeChecksum();
                    if( _ownsBaseStream )
                        _baseStream.Dispose();
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
