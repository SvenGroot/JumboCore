// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using System.Globalization;

namespace NameServerApplication
{
    class ChecksumOutputStream : Stream
    {
        private readonly Stream _baseStream;
        private readonly Crc32Checksum _crc = new Crc32Checksum();
        private readonly string _crcFileName;
        private readonly byte[] _crcBytes = new byte[4];

        public ChecksumOutputStream(Stream baseStream, string crcFileName, long startCrc)
        {
            _baseStream = baseStream;
            _crc.Value = startCrc;
            _crcFileName = crcFileName;
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
            get { return _baseStream.CanWrite; }
        }

        public override void Flush()
        {
            _baseStream.Flush();
            using( FileStream crcStream = File.Create(_crcFileName) )
            {
                uint crc = (uint)_crc.Value;
                _crcBytes[0] = (byte)(crc & 0xFF);
                _crcBytes[1] = (byte)((crc >> 8) & 0xFF);
                _crcBytes[2] = (byte)((crc >> 16) & 0xFF);
                _crcBytes[3] = (byte)((crc >> 24) & 0xFF);
                crcStream.Write(_crcBytes, 0, 4);
            }
        }

        public override long Length
        {
            get { return _baseStream.Length; }
        }

        public override long Position
        {
            get
            {
                return _baseStream.Position;
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        public long Crc
        {
            get { return _crc.Value; }
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
            _baseStream.Write(buffer, offset, count);
            _crc.Update(buffer, offset, count);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
                _baseStream.Dispose();
        }

        public static long CheckCrc(string file)
        {
            byte[] crcBytes = new byte[4];
            using( FileStream crcStream = File.OpenRead(file + ".crc") )
            {
                if( crcStream.Read(crcBytes, 0, 4) != 4 )
                    throw new DfsException(string.Format(CultureInfo.InvariantCulture, "{0} CRC file is corrupt.", file));
            }
            uint expectedCrc = (uint)crcBytes[0] | (uint)crcBytes[1] << 8 | (uint)crcBytes[2] << 16 | (uint)crcBytes[3] << 24;

            using( FileStream stream = File.OpenRead(file) )
            {
                Crc32Checksum crc = new Crc32Checksum();
                byte[] buffer = new byte[4096];
                int bytesRead;
                while( (bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0 )
                {
                    crc.Update(buffer, 0, bytesRead);
                }
                if( crc.Value != expectedCrc )
                    throw new DfsException(string.Format(CultureInfo.InvariantCulture, "{0} is corrupt (expected CRC 0x{1:x}, actual 0x{2:x}).", file, expectedCrc, crc.Value));

                return crc.Value;
            }
        }
    }
}
