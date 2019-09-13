// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Writes records to a stream as plain text.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    public class TextRecordWriter<T> : StreamRecordWriter<T>
    {
        private readonly byte[] _recordSeparator;
        private readonly Encoder _encoder = Encoding.UTF8.GetEncoder();
        private readonly char[] _charBuffer = new char[1024];
        private readonly byte[] _byteBuffer;
        private readonly bool _utf8StringRecords = typeof(T) == typeof(Utf8String);

        /// <summary>
        /// Initializes a new instance of the <see cref="TextRecordWriter{T}"/> class with the specified
        /// stream and a new line record separator.
        /// </summary>
        /// <param name="stream">The stream to write the records to.</param>
        public TextRecordWriter(Stream stream)
            : this(stream, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TextRecordWriter{T}"/> class with the specified
        /// stream and record separator.
        /// </summary>
        /// <param name="stream">The stream to write the records to.</param>
        /// <param name="recordSeparator">The character sequence to write between every record. May be <see langword="null"/> to
        /// use the default value of <see cref="Environment.NewLine"/>.</param>
        public TextRecordWriter(Stream stream, string recordSeparator)
            : base(stream)
        {
            _recordSeparator = Encoding.UTF8.GetBytes(recordSeparator ?? Environment.NewLine);
            _byteBuffer = new byte[Encoding.UTF8.GetMaxByteCount(_charBuffer.Length)];
        }

        /// <summary>
        /// Writes the specified record to the stream.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(T record)
        {
            if( _utf8StringRecords )
            {
                // Doing (Utf8String)record is not allowed on generic type arguments.
                // No need to check for null because we already know it must be a Utf8String.
                (record as Utf8String).Write(Stream);
            }
            else
            {
                string recordString = record.ToString();
                int charsLeft = recordString.Length;
                int index = 0;
                while( charsLeft > 0 )
                {
                    int copySize = Math.Min(charsLeft, _charBuffer.Length);
                    recordString.CopyTo(index, _charBuffer, 0, copySize);
                    int byteCount = _encoder.GetBytes(_charBuffer, 0, copySize, _byteBuffer, 0, charsLeft == copySize);
                    Stream.Write(_byteBuffer, 0, byteCount);
                    charsLeft -= copySize;
                    index += copySize;
                }
            }

            Stream.Write(_recordSeparator, 0, _recordSeparator.Length);

            base.WriteRecordInternal(record);
        }
    }
}
