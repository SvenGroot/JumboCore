// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// An implementation of <see cref="RecordInput"/> that reads the input from an existing stream.
    /// </summary>
    public class StreamRecordInput : RecordInput
    {
        private readonly Type _recordReaderType;
        private readonly Stream _stream;
        private readonly bool _isMemoryBased;
        private readonly string _sourceName;
        private readonly bool _inputContainsRecordSizes;
        private readonly bool _allowRecordReuse;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamRecordInput"/> class.
        /// </summary>
        /// <param name="recordReaderType">Type of the record reader. This must be a specialization of the <see cref="BinaryRecordReader{T}"/> generic class.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="isMemoryBased">if set to <see langword="true"/>, the input is read from memory and not from disk.</param>
        /// <param name="sourceName">Name of the source.</param>
        /// <param name="inputContainsRecordSizes">if set to <see langword="true"/> the input data contains record size markers.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/> [allow record reuse].</param>
        public StreamRecordInput(Type recordReaderType, Stream stream, bool isMemoryBased, string sourceName, bool inputContainsRecordSizes, bool allowRecordReuse)
        {
            _recordReaderType = recordReaderType;
            _stream = stream;
            _isMemoryBased = isMemoryBased;
            _sourceName = sourceName;
            _inputContainsRecordSizes = inputContainsRecordSizes;
            _allowRecordReuse = allowRecordReuse;
        }

        /// <summary>
        /// Gets a value indicating whether this input is read from memory.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this input is read from memory; <see langword="false"/> if it is read from a file.
        /// </value>
        public override bool IsMemoryBased
        {
            get { return _isMemoryBased; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance supports the raw record reader.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance supports the raw record reader; otherwise, <see langword="false"/>.
        /// </value>
        public override bool IsRawReaderSupported
        {
            get { return !IsReaderCreated && _inputContainsRecordSizes; }
        }

        /// <summary>
        /// Creates the record reader for this input.
        /// </summary>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected override IRecordReader CreateReader()
        {
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, _stream, 0, _stream.Length, _allowRecordReuse, _inputContainsRecordSizes);
            reader.SourceName = _sourceName;
            return reader;            
        }

        /// <summary>
        /// Creates the raw record reader for this input.
        /// </summary>
        /// <returns>
        /// The record reader for this input.
        /// </returns>
        protected override RecordReader<RawRecord> CreateRawReader()
        {
            if( !_inputContainsRecordSizes )
                throw new NotSupportedException("Cannot create a raw record reader for input without record size markers.");

            // We always allow record reuse for raw record readers. Don't specify that the input contains record sizes, because those are used by the records themselves here.
            return new BinaryRecordReader<RawRecord>(_stream, true) { SourceName = _sourceName };
        }
    }
}
