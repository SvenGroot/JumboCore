// Copyright (c) Sven Groot (Ookii.org)
using System.Diagnostics;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

sealed class SingleFileMultiRecordWriter<T> : SpillRecordWriter<T>
    where T : notnull
{
    private readonly string _outputPath;
    private readonly int _writeBufferSize;
    private readonly int _partitions;
    private long _indexBytesWritten;
    private readonly bool _enableChecksum;
    private readonly CompressionType _compressionType;

    public SingleFileMultiRecordWriter(string outputPath, IPartitioner<T> partitioner, int bufferSize, int limit, int writeBufferSize, bool enableChecksum, CompressionType compressionType)
        : base(partitioner, bufferSize, limit, SpillRecordWriterOptions.AllowRecordWrapping | SpillRecordWriterOptions.AllowMultiRecordIndexEntries)
    {
        _outputPath = outputPath;
        _partitions = partitioner.Partitions;
        _writeBufferSize = writeBufferSize;
        _enableChecksum = enableChecksum;
        _compressionType = compressionType;
        //_debugWriter = new StreamWriter(outputPath + ".debug.txt");
    }

    public override long BytesWritten
    {
        get
        {
            return base.BytesWritten + _indexBytesWritten;
        }
    }

    protected override void SpillOutput(bool finalSpill)
    {
        using (var fileStream = new FileStream(_outputPath, FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize))
        using (var indexStream = new FileStream(_outputPath + ".index", FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize))
        using (var indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream))
        {
            if (indexStream.Length == 0)
            {
                // Write a faux first entry indicating the number of partitions.
                indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L, 0L));
            }

            for (var partition = 0; partition < _partitions; ++partition)
            {
                if (HasDataForPartition(partition))
                {
                    var startOffset = fileStream.Position;
                    long uncompressedSize;
                    using (var stream = new ChecksumOutputStream(fileStream, false, _enableChecksum).CreateCompressor(_compressionType))
                    {
                        WritePartition(partition, stream);
                        var compressor = stream as ICompressor;
                        uncompressedSize = compressor == null ? stream.Length : compressor.UncompressedBytesWritten;
                    }
                    var compressedSize = fileStream.Position - startOffset;
                    Debug.Assert(uncompressedSize > 0);

                    var indexEntry = new PartitionFileIndexEntry(partition, startOffset, compressedSize, uncompressedSize);
                    indexWriter.WriteRecord(indexEntry);
                }
            }
            _indexBytesWritten = indexStream.Length;
        }
    }
}
