﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

class PartitionFileRecordInput : RecordInput
{
    private readonly Type _recordReaderType;
    private readonly string _fileName;
    private readonly string? _sourceName;
    private readonly bool _inputContainsRecordSizes;
    private readonly int _bufferSize;
    private readonly bool _allowRecordReuse;
    private readonly CompressionType _compressionType;
    private readonly IEnumerable<PartitionFileIndexEntry> _indexEntries;

    public PartitionFileRecordInput(Type recordReaderType, string fileName, IEnumerable<PartitionFileIndexEntry> indexEntries, string? sourceName, bool inputContainsRecordSizes, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
    {
        ArgumentNullException.ThrowIfNull(recordReaderType);
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentNullException.ThrowIfNull(indexEntries);

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

    protected override IRecordReader CreateReader()
    {
        var stream = new PartitionFileStream(_fileName, _bufferSize, _indexEntries, _compressionType);
        var reader = (IRecordReader)Activator.CreateInstance(_recordReaderType, stream, 0, stream.Length, _allowRecordReuse, _inputContainsRecordSizes)!;
        reader.SourceName = _sourceName;
        return reader;
    }

    protected override RecordReader<RawRecord> CreateRawReader()
    {
        if (!_inputContainsRecordSizes)
        {
            throw new NotSupportedException("Cannot create a raw record reader for input without record size markers.");
        }

        var stream = new PartitionFileStream(_fileName, _bufferSize, _indexEntries, _compressionType);
        // We always allow record reuse for raw record readers. Don't specify that the input contains record sizes, because those are used by the records themselves here.
        return new BinaryRecordReader<RawRecord>(stream, true) { SourceName = _sourceName };
    }
}
