// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.IO;
using System.Globalization;
using System.Runtime.Serialization;
using System.Diagnostics;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Multi record writer that collects the records in an in-memory buffer, and periodically spills the records to disk when the buffer is full. The final output is sorted.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Each spill is written to its own file, and each partition is sorted using <see cref="IndexedQuicksort"/> before being spilled. When <see cref="FinishWriting"/>
    ///   is called, the individual spills are merged using <see cref="MergeHelper{T}"/> into the final output file.
    /// </para>
    /// <para>
    ///   It is possible to specify a combiner task that will be run on the records of each spill after sorting. Use this to reduce the size of the output records after sorting.
    ///   The combiner must be a <see cref="ITask{TInput,TOutput}"/> where both the input and output record type are <typeparamref name="T"/>. The <see cref="ITask{TInput,TOutput}.Run"/>
    ///   method will be called multiple times (once for each spill), so the task must be prepared for. You can use a <see cref="Ookii.Jumbo.Jet.Tasks.ReduceTask{TKey,TValue,TOutput}"/> for Map-Reduce style
    ///   combining.
    /// </para>
    /// </remarks>
    public sealed class SortSpillRecordWriter<T> : SpillRecordWriter<T>
    {
        #region Nested types

        private class IndexedRecordReader : RecordReader<T>
        {
            private readonly RecordIndexEntry[] _index;
            private readonly MemoryStream _bufferStream;
            private readonly BinaryReader _reader;
            private readonly long _totalBytes;
            private readonly bool _allowRecordReuse;
            private T _record;
            private long _bytesRead;
            private int _indexPosition;

            public IndexedRecordReader(byte[] buffer, RecordIndexEntry[] index, bool allowRecordReuse)
            {
                _bufferStream = new MemoryStream(buffer, false);
                _reader = new BinaryReader(_bufferStream);
                _index = index;
                _totalBytes = index.Sum(e => e.Count);
                _allowRecordReuse = allowRecordReuse && ValueWriter<T>.Writer == null;
                if( _allowRecordReuse )
                    _record = (T)FormatterServices.GetUninitializedObject(typeof(T));
            }

            public override long InputBytes
            {
                get { return _bytesRead; }
            }

            public override float Progress
            {
                get { return (float)_bytesRead / (float)_totalBytes; }
            }

            protected override bool ReadRecordInternal()
            {
                if( _indexPosition < _index.Length )
                {
                    _bytesRead += _index[_indexPosition].Count;
                    _bufferStream.Position = _index[_indexPosition++].Offset;

                    if( _allowRecordReuse )
                    {
                        ((IWritable)_record).Read(_reader);
                        CurrentRecord = _record;
                    }
                    else
                        CurrentRecord = ValueWriter<T>.ReadValue(_reader);

                    return true;
                }
                else
                {
                    CurrentRecord = default(T);
                    return false;
                }
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _reader.Dispose();
                _bufferStream.Dispose();
            }
        }

        private sealed class CombineRecordWriter : RecordWriter<T>
        {
            private readonly RecordWriter<RawRecord> _output;
            private readonly MemoryStream _serializationBuffer = new MemoryStream();
            private readonly BinaryWriter _serializationWriter;
            private readonly RawRecord _record = new RawRecord();

            public CombineRecordWriter(RecordWriter<RawRecord> output)
            {
                _output = output;
                _serializationWriter = new BinaryWriter(_serializationBuffer);
            }

            protected override void WriteRecordInternal(T record)
            {
                _serializationBuffer.Position = 0;
                _serializationBuffer.SetLength(0);
                ValueWriter<T>.WriteValue(record, _serializationWriter);

                _record.Reset(_serializationBuffer.GetBuffer(), 0, (int)_serializationBuffer.Length);
                _output.WriteRecord(_record);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _serializationWriter.Dispose();
                _serializationBuffer.Dispose();
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(SortSpillRecordWriter<>));

        private readonly string _outputPath;
        private readonly string _outputPathBase;
        private readonly int _writeBufferSize;
        private readonly int _partitions;
        private readonly bool _enableChecksum;
        private readonly List<string> _spillFiles = new List<string>();
        private readonly List<PartitionFileIndexEntry>[] _spillPartitionIndices;
        private readonly int _maxDiskInputsPerMergePass;
        private readonly ITask<T, T> _combiner;
        private readonly bool _combinerAllowsRecordReuse;
        private readonly int _minSpillsForCombineDuringMerge;
        private readonly IRawComparer<T> _comparer;
        private readonly CompressionType _compressionType;
        private long _bytesWritten;
        private long _bytesRead;

        /// <summary>
        /// Initializes a new instance of the <see cref="SortSpillRecordWriter&lt;T&gt;" /> class.
        /// </summary>
        /// <param name="outputPath">The path of the output file.</param>
        /// <param name="partitioner">The partitioner for the records.</param>
        /// <param name="bufferSize">The size of the in-memory buffer.</param>
        /// <param name="limit">The amount of data in the buffer when a spill is triggered.</param>
        /// <param name="writeBufferSize">Size of the buffer to use for writing to disk.</param>
        /// <param name="enableChecksum">if set to <see langword="true" /> checksum calculation is enabled on all files.</param>
        /// <param name="compressionType">Type of the compression.</param>
        /// <param name="maxDiskInputsPerMergePass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">A <see cref="IRawComparer{T}"/> or <see cref="IComparer{T}"/> to use when comparing. Using <see cref="IRawComparer{T}"/> is strongly recommended. May be <see langword="null"/></param>
        /// <param name="combiner">The combiner to use during spills. May be <see langword="null" />.</param>
        /// <param name="minSpillsForCombineDuringMerge">The minimum number of spills needed for the combiner to rerun during merge. If this value is 0, the combiner will never be run during the merge. Ignored when <paramref name="combiner" /> is <see langword="null" />.</param>
        public SortSpillRecordWriter(string outputPath, IPartitioner<T> partitioner, int bufferSize, int limit, int writeBufferSize, bool enableChecksum, CompressionType compressionType, int maxDiskInputsPerMergePass, IComparer<T> comparer = null, ITask<T, T> combiner = null, int minSpillsForCombineDuringMerge = 0)
            : base(partitioner, bufferSize, limit, SpillRecordWriterOptions.None)
        {
            if( outputPath == null )
                throw new ArgumentNullException("outputPath");
            if( writeBufferSize < 0 )
                throw new ArgumentOutOfRangeException("writeBufferSize");
            if( minSpillsForCombineDuringMerge < 0 )
                throw new ArgumentOutOfRangeException("minSpillsForCombineDuringMerge");
            if( partitioner == null )
                throw new ArgumentNullException("partitioner");
            _outputPath = outputPath;
            _partitions = partitioner.Partitions;
            _outputPathBase = Path.Combine(Path.GetDirectoryName(_outputPath), Path.GetFileNameWithoutExtension(_outputPath));
            _writeBufferSize = writeBufferSize;
            _enableChecksum = enableChecksum;
            _maxDiskInputsPerMergePass = maxDiskInputsPerMergePass;
            _combiner = combiner;
            _minSpillsForCombineDuringMerge = minSpillsForCombineDuringMerge;
            _spillPartitionIndices = new List<PartitionFileIndexEntry>[_partitions];
            _compressionType = compressionType;
            _comparer = comparer as IRawComparer<T>;
            if( _comparer == null )
            {
                if( comparer != null )
                    _comparer = RawComparer<T>.CreateDeserializingComparer(comparer);
                else
                    _comparer = RawComparer<T>.CreateComparer();
            }
            for( int x = 0; x < _spillPartitionIndices.Length; ++x )
                _spillPartitionIndices[x] = new List<PartitionFileIndexEntry>();

            if( combiner != null )
            {
                AllowRecordReuseAttribute attribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(combiner.GetType(), typeof(AllowRecordReuseAttribute));
                _combinerAllowsRecordReuse = (attribute != null); // PassThrough doesn't matter, since the combiner's output always allows record reuse.
            }
        }

        /// <summary>
        /// Gets the number of bytes that were actually written to the output.
        /// </summary>
        /// <value>The number of bytes written to the output.</value>
        public override long BytesWritten
        {
            get { return _bytesWritten; }
        }

        /// <summary>
        /// Gets the number of bytes read during merging.
        /// </summary>
        public long BytesRead
        {
            get { return _bytesRead; }
        }

        /// <summary>
        /// Informs the record writer that no further records will be written.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Performs the final spill, if one is needed, and then merges the spills into the final sorted output.
        /// </para>
        /// </remarks>
        public override void FinishWriting()
        {
            if( !HasFinishedWriting )
            {
                base.FinishWriting(); // Performs the final spill.
                MergeSpills();
            }
        }

        /// <summary>
        /// Writes the spill data to the output.
        /// </summary>
        /// <param name="finalSpill">If set to <see langword="true"/>, this is the final spill.</param>
        protected override void SpillOutput(bool finalSpill)
        {
            string spillFile = string.Format(CultureInfo.InvariantCulture, "{0}_spill{1}.tmp", _outputPathBase, SpillCount);
            using( FileStream fileStream = File.Create(spillFile, _writeBufferSize) )
            {
                for( int partition = 0; partition < _partitions; ++partition )
                {
                    if( HasDataForPartition(partition) )
                    {
                        long startOffset = fileStream.Position;
                        long uncompressedSize;
                        using( Stream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum).CreateCompressor(_compressionType) )
                        using( BinaryRecordWriter<RawRecord> writer = new BinaryRecordWriter<RawRecord>(stream) )
                        {
                            if( _combiner == null )
                                WritePartition(partition, writer);
                            else
                                CombinePartition(partition, writer);
                            ICompressor compressor = stream as ICompressor;
                            uncompressedSize = compressor == null ? stream.Length : compressor.UncompressedBytesWritten;
                        }
                        long compressedSize = fileStream.Position - startOffset;
                        Debug.Assert(uncompressedSize > 0);

                        PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, compressedSize, uncompressedSize);
                        _spillPartitionIndices[partition].Add(indexEntry);
                    }
                    else
                        _spillPartitionIndices[partition].Add(new PartitionFileIndexEntry()); // Add a blank index entry so the merger can tell there's no data here.
                }
                _bytesWritten += fileStream.Length;
            }
            _spillFiles.Add(spillFile);
        }

        /// <summary>
        /// Sorts the partition before spilling.
        /// </summary>
        /// <param name="partition">The partition number.</param>
        /// <param name="index">The index entries for this partition.</param>
        /// <param name="buffer">The buffer containing the spill data.</param>
        protected override void PreparePartition(int partition, RecordIndexEntry[] index, byte[] buffer)
        {
            base.PreparePartition(partition, index, buffer);
            _log.DebugFormat("Sorting partition {0}.", partition);
            IndexedQuicksort.Sort(index, buffer, _comparer);
            _log.Debug("Sort complete.");
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DeleteTempFiles();
        }

        private void CombinePartition(int partition, BinaryRecordWriter<RawRecord> output)
        {
            RecordIndexEntry[] index = GetSpillIndex(partition);
            if( index != null )
            {
                byte[] buffer = SpillBuffer;
                PreparePartition(partition, index, buffer);
                using( IndexedRecordReader input = new IndexedRecordReader(buffer, index, _combinerAllowsRecordReuse) )
                using( CombineRecordWriter combineOutput = new CombineRecordWriter(output) )
                {
                    _combiner.Run(input, combineOutput);
                }
            }
        }

        private void MergeSpills()
        {
            if( _spillFiles.Count == 1 )
            {
                UseSingleSpillAsOutput();     
            }
            else
            {
                MergeMultipleSpills();
            }
        }

        private void MergeMultipleSpills()
        {
            string intermediateOutputPath = Path.GetDirectoryName(_outputPath);
            List<RecordInput> diskInputs = new List<RecordInput>(_spillFiles.Count);
            MergeHelper<T> merger = new MergeHelper<T>();
            using( FileStream fileStream = File.Create(_outputPath, _writeBufferSize) )
            using( FileStream indexStream = File.Create(_outputPath + ".index", _writeBufferSize) )
            using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
            {
                // Write a faux first entry indicating the number of partitions.
                indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L, 0L));

                for( int partition = 0; partition < _partitions; ++partition )
                {
                    MergePartition(intermediateOutputPath, diskInputs, merger, fileStream, indexWriter, partition);
                }
                _bytesWritten += fileStream.Length + indexStream.Length + merger.BytesWritten;
                _bytesRead = merger.BytesRead;
            }

            DeleteTempFiles();
            _log.Info("Merge complete.");
        }

        private void MergePartition(string intermediateOutputPath, List<RecordInput> diskInputs, MergeHelper<T> merger, FileStream fileStream, BinaryRecordWriter<PartitionFileIndexEntry> indexWriter, int partition)
        {
            if( _spillPartitionIndices[partition].Any(i => i.UncompressedSize > 0) )
            {
                _log.InfoFormat("Merging partition {0}", partition);
                long startOffset = fileStream.Position;
                long uncompressedSize;
                using( Stream stream = new ChecksumOutputStream(fileStream, false, _enableChecksum).CreateCompressor(_compressionType) )
                using( BinaryRecordWriter<RawRecord> writer = new BinaryRecordWriter<RawRecord>(stream) )
                {
                    diskInputs.Clear();
                    for( int x = 0; x < _spillFiles.Count; ++x )
                    {
                        if( _spillPartitionIndices[partition][x].UncompressedSize > 0 )
                            diskInputs.Add(new PartitionFileRecordInput(typeof(BinaryRecordReader<T>), _spillFiles[x], new[] { _spillPartitionIndices[partition][x] }, null, true, true, _writeBufferSize, _compressionType));
                    }

                    bool runCombiner = !(_combiner == null || _minSpillsForCombineDuringMerge == 0 || SpillCount < _minSpillsForCombineDuringMerge);
                    bool allowRecordReuse = !runCombiner || _combinerAllowsRecordReuse;
                    MergeResult<T> mergeResult = merger.Merge(diskInputs, null, _maxDiskInputsPerMergePass, _comparer, allowRecordReuse, runCombiner, intermediateOutputPath, "", CompressionType.None, _writeBufferSize, _enableChecksum);
                    if( runCombiner )
                    {
                        using( EnumerableRecordReader<T> combineInput = new EnumerableRecordReader<T>(mergeResult.Select(r => r.GetValue()), 0) )
                        using( CombineRecordWriter combineOutput = new CombineRecordWriter(writer) )
                        {
                            _combiner.Run(combineInput, combineOutput);
                        }
                    }
                    else
                    {
                        foreach( MergeResultRecord<T> record in mergeResult )
                        {
                            record.WriteRawRecord(writer);
                        }
                    }

                    ICompressor compressor = stream as ICompressor;
                    uncompressedSize = compressor == null ? stream.Length : compressor.UncompressedBytesWritten;
                }
                PartitionFileIndexEntry indexEntry = new PartitionFileIndexEntry(partition, startOffset, fileStream.Position - startOffset, uncompressedSize);
                Debug.Assert(indexEntry.UncompressedSize > 0);
                indexWriter.WriteRecord(indexEntry);
            }
        }

        private void UseSingleSpillAsOutput()
        {
            File.Move(_spillFiles[0], _outputPath);
            using( FileStream indexStream = File.Create(_outputPath + ".index", _writeBufferSize) )
            using( BinaryRecordWriter<PartitionFileIndexEntry> indexWriter = new BinaryRecordWriter<PartitionFileIndexEntry>(indexStream) )
            {
                // Write a faux first entry indicating the number of partitions.
                indexWriter.WriteRecord(new PartitionFileIndexEntry(_partitions, 0L, 0L, 0L));

                for( int partition = 0; partition < _partitions; ++partition )
                {
                    if( _spillPartitionIndices[partition][0].UncompressedSize > 0 )
                        indexWriter.WriteRecord(_spillPartitionIndices[partition][0]);
                }

                _bytesWritten += indexStream.Length;
            }
        }

        private void DeleteTempFiles()
        {
            foreach( string fileName in _spillFiles )
            {
                if( File.Exists(fileName) )
                    File.Delete(fileName);
            }
            _spillFiles.Clear();
        }
    }
}
