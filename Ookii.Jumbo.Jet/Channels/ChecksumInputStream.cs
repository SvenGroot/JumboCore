// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class ChecksumInputStream : Stream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(ChecksumInputStream));

        private readonly Stream _baseStream;
        private readonly bool _ownsBaseStream;
        private Crc32Checksum _checksum;
        private readonly long _length;
        private readonly string _fileName;
        private readonly bool _deleteFile;
        private long _position;
        private bool _disposed;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public ChecksumInputStream(string fileName, int bufferSize, bool deleteFile)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize), true)
        {
            _fileName = fileName;
            _deleteFile = deleteFile;
        }

        public ChecksumInputStream(Stream baseStream, bool ownsBaseStream, long? length = null)
        {
            if( baseStream == null )
                throw new ArgumentNullException(nameof(baseStream));

            _baseStream = baseStream;
            _ownsBaseStream = ownsBaseStream;

            if( (length ?? _baseStream.Length) > 0 )
            {
                _length = (length ?? _baseStream.Length) - 1;
                bool enableChecksum = baseStream.ReadByte() == 1;
                if( enableChecksum )
                {
                    _checksum = new Crc32Checksum();
                    _length -= sizeof(uint);
                    if( _length < 0 )
                        throw new IOException("Invalid checksum stream.");
                }
            }
        }

        public bool IsChecksumEnabled
        {
            get { return _checksum != null; }
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
            get { return false; }
        }

        public override void Flush()
        {
            _baseStream.Flush();
        }

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if( _disposed )
                throw new ObjectDisposedException(typeof(ChecksumInputStream).FullName);
            count = (int)Math.Min(count, _length - _position);

            if( count == 0 )
                return 0;
            else
            {
                int bytesRead = _baseStream.Read(buffer, offset, count);
                _position += bytesRead;

                if( _checksum != null )
                {
                    _checksum.Update(buffer, offset, bytesRead);
                    if( _position == _length )
                    {
                        byte[] sum = new byte[sizeof(uint)];
                        int sumBytesRead = _baseStream.Read(sum, 0, sum.Length);
                        if( sumBytesRead != sum.Length || _checksum.ValueUInt32 != BitConverter.ToUInt32(sum, 0) )
                            throw new IOException("Invalid checksum on input stream."); // TODO: More specific exception
                    }
                }

                return bytesRead;
            }
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
            throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed )
                {
                    if( _position < _length && _checksum != null )
                    {
                        // Need to read to the end to verify the checksum
                        byte[] buffer = new byte[4096];
                        while( Read(buffer, 0, buffer.Length) > 0 )
                        {
                        }
                        Debug.Assert(_position == _length);
                    }
                    _disposed = true;
                    if( _ownsBaseStream )
                        _baseStream.Dispose();

                    if( _deleteFile )
                    {
                        try
                        {
                            if( File.Exists(_fileName) )
                            {
                                File.Delete(_fileName);
                            }
                        }
                        catch( IOException ex )
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                        }
                        catch( UnauthorizedAccessException ex )
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                        }
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
