using System;
using System.Globalization;
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DfsWeb.Pages
{
    public class ViewFileModel : PageModel
    {
        #region Nested types

        private class SizeLimitedStream : Stream
        {
            private readonly Stream _baseStream;
            private readonly long _size;

            public SizeLimitedStream(Stream baseStream, long size)
            {
                _baseStream = baseStream;
                _size = size;
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
                get { return Math.Min(_size, _baseStream.Length); }
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

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (Position + count > _size)
                {
                    count = (int)(_size - Position);
                    if (count < 0)
                        count = 0;
                }


                return _baseStream.Read(buffer, offset, count);
            }

            public override int ReadByte()
            {
                if (Position + 1 > _size)
                {
                    return -1;
                }

                return _baseStream.ReadByte();
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
                base.Dispose(disposing);
                if (disposing)
                {
                    if (_baseStream != null)
                    {
                        _baseStream.Dispose();
                    }
                }
            }
        }


        #endregion

        [BindProperty(SupportsGet = true)]
        public string Path { get; set; }

        [BindProperty(SupportsGet = true)]
        public string MaxSize { get; set; }

        [BindProperty(SupportsGet = true)]
        public bool Tail { get; set; }

        public string HeaderText { get; set; }

        public string FileContents { get; set; }

        public bool Error { get; set; }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "<Pending>")]
        public void OnGet()
        {
            var maxSize = MaxSize == null ? 100 * BinarySize.Kilobyte : (long)BinarySize.Parse(MaxSize, CultureInfo.InvariantCulture);
            HeaderText = string.Format(CultureInfo.InvariantCulture, "File '{0}' contents ({1} {2})", Path, Tail ? "last" : "first", new BinarySize(maxSize));
            try
            {
                var client = FileSystemClient.Create();
                using (var stream = client.OpenFile(Path))
                {
                    if (Tail)
                        stream.Position = Math.Max(0, stream.Length - maxSize);
                    using (var sizeStream = new SizeLimitedStream(stream, Tail ? stream.Length : maxSize))
                    using (var reader = new StreamReader(sizeStream))
                    {
                        FileContents = reader.ReadToEnd();
                    }
                }

                ViewData["Title"] = Path;
            }
            catch (Exception ex)
            {
                FileContents = ex.ToString();
                Error = true;
            }
        }
    }
}
