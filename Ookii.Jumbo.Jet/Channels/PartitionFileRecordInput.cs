// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    class PartitionFileRecordInput : RecordInput
    {
        private readonly Type _recordReaderType;
        private readonly string _fileName;
        private readonly string _sourceName;
        private readonly bool _inputContainsRecordSizes;
        private readonly int _bufferSize;
        private readonly bool _allowRecordReuse;
        private readonly CompressionType _compressionType;
        private readonly IEnumerable<PartitionFileIndexEntry> _indexEntries;

        public PartitionFileRecordInput(Type recordReaderType, string fileName, IEnumerable<PartitionFileIndexEntry> indexEntries, string sourceName, bool inputContainsRecordSizes, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
        {
            if (recordReaderType == null)
                throw new ArgumentNullException(nameof(recordReaderType));
            if (fileName == null)
                throw new ArgumentNullException(nameof(fileName));
            if (indexEntries == null)
                throw new ArgumentNullException(nameof(indexEntries));

            _recordReaderType = recordReaderType;
            _fileName = fileName;
            _indexEntries = indexEntries;
            _sourceName = sourceName;
            _inputContainsRecordSizes = inputContainsRecordSizes;
            _bufferSize = bufferSize;
            _allowRecordReuse = allowRecordReuse;
            _compressionType = compressionType;
        }

        public override bool IsMemoryBased
        {
            get { return false; }
        }

        public override bool IsRawReaderSupported
        {
            get { return !IsReaderCreated && _inputContainsRecordSizes; }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        protected override IRecordReader CreateReader()
        {
            PartitionFileStream stream = new PartitionFileStream(_fileName, _bufferSize, _indexEntries, _compressionType);
            IRecordReader reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, stream, 0, stream.Length, _allowRecordReuse, _inputContainsRecordSizes);
            reader.SourceName = _sourceName;
            return reader;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        protected override RecordReader<RawRecord> CreateRawReader()
        {
            if (!_inputContainsRecordSizes)
                throw new NotSupportedException("Cannot create a raw record reader for input without record size markers.");

            PartitionFileStream stream = new PartitionFileStream(_fileName, _bufferSize, _indexEntries, _compressionType);
            // We always allow record reuse for raw record readers. Don't specify that the input contains record sizes, because those are used by the records themselves here.
            return new BinaryRecordReader<RawRecord>(stream, true) { SourceName = _sourceName };
        }
    }
}
