// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    class SegmentedChecksumInputStream : Stream
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SegmentedChecksumInputStream));

        private readonly Stream _baseStream;
        private readonly long _length;
        private readonly string _fileName;
        private readonly bool _deleteFile;
        private readonly byte[] _sizeBuffer = new byte[sizeof(long)];
        private readonly CompressionType _compressionType;
        private Stream _currentSegment;
        private long _position;
        private bool _disposed;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public SegmentedChecksumInputStream(string fileName, int bufferSize, bool deleteFile, int segmentCount, CompressionType compressionType, long uncompressedSize)
            : this(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize), segmentCount, compressionType, uncompressedSize)
        {
            _fileName = fileName;
            _deleteFile = deleteFile;
        }

        public SegmentedChecksumInputStream(Stream baseStream, int segmentCount, CompressionType compressionType, long uncompressedSize)
        {
            if (baseStream == null)
                throw new ArgumentNullException(nameof(baseStream));
            if (segmentCount < 1)
                throw new ArgumentOutOfRangeException(nameof(segmentCount));
            _baseStream = baseStream;
            _length = uncompressedSize;
            _compressionType = compressionType;
            NextSegment();

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
            if (_disposed)
                throw new ObjectDisposedException(typeof(SegmentedChecksumInputStream).FullName);

            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (_currentSegment == null) // Can only happen if the stream was empty
                return 0;

            var totalBytesRead = 0;
            while (count > 0)
            {
                var bytesRead = _currentSegment.Read(buffer, offset, count);
                if (bytesRead == 0 && !NextSegment())
                    break;
                count -= bytesRead;
                offset += bytesRead;
                _position += bytesRead;
                totalBytesRead += bytesRead;
            }

            return totalBytesRead;
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
                if (!_disposed)
                {
                    _disposed = true;
                    if (_currentSegment != null)
                    {
                        _currentSegment.Dispose();
                        _currentSegment = null;
                    }
                    _baseStream.Dispose();
                    if (_deleteFile)
                    {
                        try
                        {
                            if (File.Exists(_fileName))
                            {
                                File.Delete(_fileName);
                            }
                        }
                        catch (IOException ex)
                        {
                            _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Failed to delete file {0}.", _fileName), ex);
                        }
                        catch (UnauthorizedAccessException ex)
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

        private bool NextSegment()
        {
            if (_currentSegment != null)
                _currentSegment.Dispose();

            if (_position < _length)
            {
                var segmentLength = ReadInt64();
                var uncompressedLength = ReadInt64();
                _currentSegment = new ChecksumInputStream(_baseStream, false, segmentLength).CreateDecompressor(_compressionType, uncompressedLength);
                return true;
            }
            else
                return false;
        }

        private long ReadInt64()
        {
            var bytesRead = _baseStream.Read(_sizeBuffer, 0, _sizeBuffer.Length);
            if (bytesRead < _sizeBuffer.Length)
                throw new IOException("Invalid segmented stream.");

            return BitConverter.ToInt64(_sizeBuffer, 0);
        }
    }
}
