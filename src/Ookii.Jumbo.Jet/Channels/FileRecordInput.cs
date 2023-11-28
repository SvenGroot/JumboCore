// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

class FileRecordInput : RecordInput
{
    private readonly Type _recordReaderType;
    private readonly string _fileName;
    private readonly string? _sourceName;
    private readonly long _uncompressedSize;
    private readonly bool _deleteFile;
    private readonly bool _inputContainsRecordSizes;
    private readonly int _segmentCount;
    private readonly bool _allowRecordReuse;
    private readonly int _bufferSize;
    private readonly CompressionType _compressionType;

    public FileRecordInput(Type recordReaderType, string fileName, string? sourceName, long uncompressedSize, bool deleteFile, bool inputContainsRecordSizes, int segmentCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
    {
        ArgumentNullException.ThrowIfNull(recordReaderType);
        ArgumentNullException.ThrowIfNull(fileName);

        _recordReaderType = recordReaderType;
        _fileName = fileName;
        _sourceName = sourceName;
        _uncompressedSize = uncompressedSize;
        _deleteFile = deleteFile;
        _inputContainsRecordSizes = inputContainsRecordSizes;
        _segmentCount = segmentCount;
        _bufferSize = bufferSize;
        _compressionType = compressionType;
        _allowRecordReuse = allowRecordReuse;
    }

    public override bool IsMemoryBased
    {
        get { return false; }
    }

    public override bool IsRawReaderSupported
    {
        get { return !IsReaderCreated && _inputContainsRecordSizes; }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership handed to returned record reader.")]
    protected override IRecordReader CreateReader()
    {
        var stream = CreateStream();
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

        var stream = CreateStream();
        // We always allow record reuse for raw record readers. Don't specify that the input contains record sizes, because those are used by the records themselves here.
        return new BinaryRecordReader<RawRecord>(stream, true) { SourceName = _sourceName };
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            if (_deleteFile && File.Exists(_fileName))
            {
                File.Delete(_fileName);
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
    private Stream CreateStream()
    {
        Stream stream;
        if (_segmentCount == 0)
        {
            stream = new ChecksumInputStream(_fileName, _bufferSize, _deleteFile).CreateDecompressor(_compressionType, _uncompressedSize);
        }
        else
        {
            stream = new SegmentedChecksumInputStream(_fileName, _bufferSize, _deleteFile, _segmentCount, _compressionType, _uncompressedSize);
        }

        return stream;
    }

}
