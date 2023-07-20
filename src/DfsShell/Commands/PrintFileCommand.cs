// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.ComponentModel;
using System.IO;
using Ookii;
using Ookii.CommandLine;
using Ookii.CommandLine.Commands;
using Ookii.Jumbo;
using Ookii.Jumbo.IO;

namespace DfsShell.Commands
{
    [GeneratedParser]
    [Command("cat"), Description("Prints a text file.")]
    partial class PrintFileCommand : DfsShellCommand
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

        [CommandLineArgument(IsPositional = true, IsRequired = true)]
        [Description("The path of the text file on the DFS.")]
        public string Path { get; set; }

        [CommandLineArgument(DefaultValue = "utf-8"), Description("The text encoding to use. The default value is utf-8.")]
        public string Encoding { get; set; }

        [CommandLineArgument(DefaultValue = long.MaxValue), Description("The maximum number of bytes to read from the file. If not specified, the entire file will be read.")]
        public BinarySize Size { get; set; }

        [CommandLineArgument, Description("Prints the end rather than the start of the file up to the specified size.")]
        public bool Tail { get; set; }

        [CommandLineArgument, Description("Specified the type of record reader to use to read the contents of the file. This must be the assembly-qualified mangled name.")]
        public string RecordReaderType { get; set; }

        public override int Run()
        {
            if (RecordReaderType != null)
                return PrintRecordReader();
            else
            {
                var encoding = System.Text.Encoding.GetEncoding(Encoding);

                using (var stream = Client.OpenFile(Path))
                {
                    if (Tail)
                    {
                        var newPosition = stream.Length - (long)Size;
                        if (newPosition > 0)
                            stream.Position = newPosition;
                    }
                    using (var limitedStream = new SizeLimitedStream(stream, Tail ? long.MaxValue : (long)Size))
                    using (var reader = new StreamReader(limitedStream, encoding))
                    using (var writer = LineWrappingTextWriter.ForConsoleOut())
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                            writer.WriteLine(line);
                    }
                }

                return 0;
            }
        }

        private int PrintRecordReader()
        {
            var recordReaderType = Type.GetType(RecordReaderType);
            if (recordReaderType == null)
            {
                Console.Error.WriteLine("Could not load the record reader type.");
                return 1;
            }

            var recordReaderBaseType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), false);
            if (recordReaderBaseType == null)
            {
                Console.Error.WriteLine("The specified type is not a record reader.");
                return 1;
            }

            using (var stream = Client.OpenFile(Path))
            {
                IRecordReader reader = null;
                if (Size.Value < stream.Length)
                {
                    if (recordReaderType.GetConstructor(new[] { typeof(Stream), typeof(long), typeof(long), typeof(bool) }) == null)
                    {
                        Console.Error.WriteLine("No constructor found on the specified record reader type that could be used when the size argument is specified (need constructor with arguments (Stream input, long offset, long size, bool allowRecordReuse)).");
                        return 1;
                    }

                    var offset = Tail ? 0 : stream.Length - (long)Size;
                    if (offset < 0)
                        offset = 0;
                    reader = (IRecordReader)Activator.CreateInstance(recordReaderType, stream, offset, Size, true);
                }
                else
                {
                    if (recordReaderType.GetConstructor(new[] { typeof(Stream), typeof(bool) }) != null)
                    {
                        reader = (IRecordReader)Activator.CreateInstance(recordReaderType, stream, true);
                    }
                    else if (recordReaderType.GetConstructor(new[] { typeof(Stream) }) != null)
                    {
                        reader = (IRecordReader)Activator.CreateInstance(recordReaderType, stream);
                    }
                    else
                    {
                        Console.Error.WriteLine("No suitable constructor found on the specified record reader type (need constructor with Stream argument).");
                        return 1;
                    }
                }

                while (reader.ReadRecord())
                {
                    Console.WriteLine(reader.CurrentRecord);
                }
            }

            return 0;
        }
    }
}
