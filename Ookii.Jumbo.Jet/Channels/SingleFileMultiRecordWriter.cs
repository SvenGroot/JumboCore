// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class SingleFileMultiRecordWriter<T> : SpillRecordWriter<T>
    {
        private string _outputPath;
        private int _writeBufferSize;
        private int _partitions;
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
            using( FileStream fileStream = new FileStream(_outputPath, FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize) )
            using( FileStream indexStream = new FileStream(_outputPath + ".index", FileMode.Append, FileAccess.Write, FileShare.None, _writeBufferSize) )
            using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
            {
                if( indexStream.Length == 0 )
                {
                    // Write a faux first entry indicating the number of partitions.
                    indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L, 0L));
                }

                for( int partition = 0; partition < _partitions; ++partition )
                {
                    if( HasDataForPartition(partition) )
                    {
                        long startOffset = fileStream.Position;
                        long uncompressedSize;
                        using( Stream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum).CreateCompressor(_compressionType) )
                        {
                            WritePartition(partition, stream);
                            ICompressor compressor = stream as ICompressor;
                            uncompressedSize = compressor == null ? stream.Length : compressor.UncompressedBytesWritten;
                        }
                        long compressedSize = fileStream.Position - startOffset;
                        Debug.Assert(uncompressedSize > 0 );

                        PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, compressedSize, uncompressedSize);
                        indexWriter.WriteRecord(indexEntry);
                    }
                }
                _indexBytesWritten = indexStream.Length;
            }
        }
    }
}
