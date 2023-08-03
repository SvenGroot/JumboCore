// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides methods to merge multiple segments of sorted input into one sorted output.
    /// </summary>
    /// <typeparam name="T">The type of the records in the segments.</typeparam>
    public class MergeHelper<T>
        where T : notnull
    {
        #region Nested types

        private sealed class MergeInput : IDisposable
        {
            public MergeInput(RecordReader<RawRecord> reader, bool isMemoryBased)
            {
                RawRecordReader = reader;
                IsMemoryBased = isMemoryBased;
            }

            public MergeInput(RecordReader<T> reader, bool isMemoryBased)
            {
                RecordReader = reader;
                IsMemoryBased = isMemoryBased;
            }

            public RecordReader<RawRecord>? RawRecordReader { get; private set; }
            public RecordReader<T>? RecordReader { get; private set; }
            public bool IsMemoryBased { get; private set; }

            public long BytesRead
            {
                get { return IsMemoryBased ? 0 : (RawRecordReader == null ? RecordReader!.BytesRead : RawRecordReader.BytesRead); }
            }

            public bool ReadRecord()
            {
                if (RawRecordReader != null)
                    return RawRecordReader.ReadRecord();
                else
                    return RecordReader!.ReadRecord();
            }

            public void GetCurrentRecord(MergeResultRecord<T> record)
            {
                if (RawRecordReader != null)
                    record.Reset(RawRecordReader.CurrentRecord!);
                else
                    record.Reset(RecordReader!.CurrentRecord!);
            }

            public void Dispose()
            {
                if (RawRecordReader != null)
                    RawRecordReader.Dispose();
                if (RecordReader != null)
                    RecordReader.Dispose();
            }
        }

        private sealed class MergeInputComparer : IComparer<MergeInput>
        {
            private readonly IRawComparer<T>? _rawComparer;
            private readonly IComparer<T>? _comparer;

            public MergeInputComparer(IRawComparer<T> rawComparer)
            {
                _rawComparer = rawComparer ?? RawComparer<T>.CreateComparer();
            }

            public MergeInputComparer(IComparer<T> comparer)
            {
                _comparer = comparer;
            }

            public int Compare(MergeInput? x, MergeInput? y)
            {
                if (x == null)
                {
                    if (y == null)
                        return 0;
                    else
                        return -1;
                }
                else if (y == null)
                    return 1;

                if (_rawComparer == null)
                    return _comparer!.Compare(x.RecordReader!.CurrentRecord, y.RecordReader!.CurrentRecord);
                else
                    return _rawComparer.Compare(x.RawRecordReader!.CurrentRecord, y.RawRecordReader!.CurrentRecord);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeHelper<>));

        private long _bytesWritten;
        private long _bytesRead;

        /// <summary>
        /// Gets the number of bytes written by the merger.
        /// </summary>
        public long BytesWritten
        {
            get { return Interlocked.Read(ref _bytesWritten); }
        }

        /// <summary>
        /// Gets the number of bytes read by the merger.
        /// </summary>
        public long BytesRead
        {
            get { return Interlocked.Read(ref _bytesRead); }
        }

        /// <summary>
        /// Gets the number of merge passes performed, including the final pass.
        /// </summary>
        public int MergePassCount { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the last call to the <see cref="Merge(IList{RecordInput},IList{RecordInput},int,IComparer{T},bool,bool,string,string,CompressionType,int,bool)"/> method
        /// used raw records.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this the last merge operation used raw records; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsUsingRawRecords { get; private set; }

        /// <summary>
        /// Merges the specified inputs.
        /// </summary>
        /// <param name="diskInputs">The disk inputs.</param>
        /// <param name="memoryInputs">The memory inputs.</param>
        /// <param name="maxDiskInputsPerPass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">The <see cref="IComparer{T}"/> to use, or <see langword="null"/> to use the default. Do not pass <see cref="Comparer{T}.Default"/>.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/>, the result of the pass will reuse the same instance of <typeparamref name="T"/> for each pass.</param>
        /// <param name="intermediateOutputPath">The path to store intermediate passes.</param>
        /// <param name="compressionType">The type of the compression to use for intermediate passes.</param>
        /// <param name="bufferSize">The buffer size to use when writing output passes.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/>, checksums will be enabled for intermediate passes.</param>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> that can be used to get the merge results.
        /// </returns>
        public MergeResult<T> Merge(IList<RecordInput> diskInputs, IList<RecordInput> memoryInputs, int maxDiskInputsPerPass, IComparer<T> comparer, bool allowRecordReuse, string intermediateOutputPath, CompressionType compressionType, int bufferSize, bool enableChecksum)
        {
            return Merge(diskInputs, memoryInputs, maxDiskInputsPerPass, comparer, allowRecordReuse, false, intermediateOutputPath, "", compressionType, bufferSize, enableChecksum);
        }

        /// <summary>
        /// Merges the specified inputs.
        /// </summary>
        /// <param name="diskInputs">The disk inputs.</param>
        /// <param name="memoryInputs">The memory inputs.</param>
        /// <param name="maxDiskInputsPerPass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">The <see cref="IComparer{T}"/> or <see cref="IRawComparer{T}"/> to use, or <see langword="null"/> to use the default. Do not manually pass <see cref="Comparer{T}.Default"/>.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/>, the result of the pass will reuse the same instance of <typeparamref name="T"/> for each pass.</param>
        /// <param name="forceDeserialization">if set to <see langword="true"/>, don't use raw comparisons for the final pass, but force deserialization.</param>
        /// <param name="intermediateOutputPath">The path to store intermediate passes.</param>
        /// <param name="passFilePrefix">The pass file prefix.</param>
        /// <param name="compressionType">The type of the compression to use for intermediate passes.</param>
        /// <param name="bufferSize">The buffer size to use when writing output passes.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/>, checksums will be enabled for intermediate passes.</param>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> that can be used to get the merge results.
        /// </returns>
        public MergeResult<T> Merge(IList<RecordInput>? diskInputs, IList<RecordInput>? memoryInputs, int maxDiskInputsPerPass, IComparer<T>? comparer, bool allowRecordReuse, bool forceDeserialization, string intermediateOutputPath, string passFilePrefix, CompressionType compressionType, int bufferSize, bool enableChecksum)
        {
            if (diskInputs == null && memoryInputs == null)
                throw new ArgumentException("diskInputs and memoryInputs cannot both be null.");
            ArgumentNullException.ThrowIfNull(intermediateOutputPath);
            ArgumentNullException.ThrowIfNull(passFilePrefix);

            // When the specified comparer is not a raw comparer or some of the inputs don't support raw records, we must use deserialization.
            // When the comparer is a raw comparer uses deserialization, we deserialize in the merger and use regular comparisons rather than raw comparisons, since that's more
            // efficient than deserializing inside the raw comparer.
            var deserializingComparer = comparer as IDeserializingRawComparer;
            var rawReaderSupported = (comparer == null || comparer is IRawComparer<T>) && (deserializingComparer == null || !deserializingComparer.UsesDeserialization) && (memoryInputs == null || memoryInputs.All(i => i.IsRawReaderSupported)) && (diskInputs == null || diskInputs.All(i => i.IsRawReaderSupported));

            var diskInputsProcessed = 0;
            if (diskInputs != null && diskInputs.Count > maxDiskInputsPerPass)
            {
                // Make a copy of the list that we can add the intermediate results to
                var actualDiskInputs = diskInputs.ToList();

                var pass = 0;
                while (actualDiskInputs.Count - diskInputsProcessed > maxDiskInputsPerPass)
                {
                    var outputFileName = Path.Combine(intermediateOutputPath, string.Format(CultureInfo.InvariantCulture, "{0}merge_pass{1}.tmp", passFilePrefix, pass));
                    var numDiskInputsForPass = GetNumDiskInputsForPass(pass, actualDiskInputs.Count - diskInputsProcessed, maxDiskInputsPerPass);
                    _log.InfoFormat("Merging {0} intermediate segments out of a total of {1} disk segments.", numDiskInputsForPass, actualDiskInputs.Count - diskInputsProcessed);
                    IEnumerable<MergeResultRecord<T>> passResult = RunMergePass(actualDiskInputs.Skip(diskInputsProcessed).Take(numDiskInputsForPass), comparer, true, rawReaderSupported, false);
                    var uncompressedSize = WriteMergePass(passResult, outputFileName, bufferSize, compressionType, enableChecksum, rawReaderSupported);
                    actualDiskInputs.Add(new FileRecordInput(typeof(BinaryRecordReader<T>), outputFileName, null, uncompressedSize, true, rawReaderSupported, 0, allowRecordReuse, bufferSize, compressionType));
                    diskInputsProcessed += numDiskInputsForPass;
                    ++pass;
                }
                diskInputs = actualDiskInputs;
            }

            var inputs = memoryInputs ?? Enumerable.Empty<RecordInput>();
            var memoryInputCount = inputs.Count();
            var diskInputCount = 0;
            if (diskInputs != null)
            {
                inputs = inputs.Concat(diskInputs.Skip(diskInputsProcessed));
                diskInputCount = diskInputs.Count - diskInputsProcessed;
            }

            IsUsingRawRecords = rawReaderSupported && !forceDeserialization;
            _log.InfoFormat("Last merge pass with {0} disk and {1} memory segments; raw records: {2}.", diskInputCount, memoryInputCount, IsUsingRawRecords);
            return RunMergePass(inputs, comparer, allowRecordReuse, IsUsingRawRecords, true);
        }

        /// <summary>
        /// Writes the result of a merge pass to the specified.
        /// </summary>
        /// <param name="stream">The stream to which the result of the pass is written.</param>
        /// <param name="diskInputs">The disk inputs.</param>
        /// <param name="memoryInputs">The memory inputs.</param>
        /// <param name="maxDiskInputsPerPass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">The <see cref="IComparer{T}"/> to use, or <see langword="null"/> to use the default. Do not pass <see cref="Comparer{T}.Default"/>.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/>, the result of the pass will reuse the same instance of <typeparamref name="T"/> for each pass.</param>
        /// <param name="intermediateOutputPath">The path to store intermediate passes.</param>
        /// <param name="passFilePrefix">The pass file prefix.</param>
        /// <param name="compressionType">The type of the compression to use for intermediate passes.</param>
        /// <param name="bufferSize">The buffer size to use when writing output passes.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/>, checksums will be enabled for intermediate passes.</param>
        /// <returns>
        /// The uncompressed size of the written data.
        /// </returns>
        public long WriteMerge(Stream stream, IList<RecordInput> diskInputs, IList<RecordInput> memoryInputs, int maxDiskInputsPerPass, IComparer<T> comparer, bool allowRecordReuse, string intermediateOutputPath, string passFilePrefix, CompressionType compressionType, int bufferSize, bool enableChecksum)
        {
            ArgumentNullException.ThrowIfNull(stream);
            IEnumerable<MergeResultRecord<T>> mergeResult = Merge(diskInputs, memoryInputs, maxDiskInputsPerPass, comparer, allowRecordReuse, false, intermediateOutputPath, passFilePrefix, compressionType, bufferSize, enableChecksum);
            return WriteMergePass(mergeResult, stream, IsUsingRawRecords);
        }

        /// <summary>
        /// Writes the result of a merge pass to the specified.
        /// </summary>
        /// <param name="fileName">The file to write the output to.</param>
        /// <param name="diskInputs">The disk inputs.</param>
        /// <param name="memoryInputs">The memory inputs.</param>
        /// <param name="maxDiskInputsPerPass">The maximum number of disk inputs per merge pass.</param>
        /// <param name="comparer">The <see cref="IComparer{T}"/> to use, or <see langword="null"/> to use the default. Do not pass <see cref="Comparer{T}.Default"/>.</param>
        /// <param name="allowRecordReuse">if set to <see langword="true"/>, the result of the pass will reuse the same instance of <typeparamref name="T"/> for each pass.</param>
        /// <param name="intermediateOutputPath">The path to store intermediate passes.</param>
        /// <param name="passFilePrefix">The pass file prefix.</param>
        /// <param name="compressionType">The type of the compression to use for intermediate passes.</param>
        /// <param name="bufferSize">The buffer size to use when writing output passes.</param>
        /// <param name="enableChecksum">if set to <see langword="true"/>, checksums will be enabled for intermediate passes.</param>
        /// <returns>
        /// The uncompressed size of the written data.
        /// </returns>
        public long WriteMerge(string fileName, IList<RecordInput>? diskInputs, IList<RecordInput>? memoryInputs, int maxDiskInputsPerPass, IComparer<T>? comparer, bool allowRecordReuse, string intermediateOutputPath, string passFilePrefix, CompressionType compressionType, int bufferSize, bool enableChecksum)
        {
            ArgumentNullException.ThrowIfNull(fileName);
            IEnumerable<MergeResultRecord<T>> mergeResult = Merge(diskInputs, memoryInputs, maxDiskInputsPerPass, comparer, allowRecordReuse, false, intermediateOutputPath, passFilePrefix, compressionType, bufferSize, enableChecksum);
            return WriteMergePass(mergeResult, fileName, bufferSize, compressionType, enableChecksum, IsUsingRawRecords);
        }

        private long WriteMergePass(IEnumerable<MergeResultRecord<T>> pass, string outputFileName, int bufferSize, CompressionType compressionType, bool enableChecksum, bool rawReaderSupported)
        {
            using (Stream fileStream = File.Create(outputFileName, bufferSize))
            using (var outputStream = new ChecksumOutputStream(fileStream, true, enableChecksum).CreateCompressor(compressionType))
            {
                return WriteMergePass(pass, outputStream, rawReaderSupported);
            }
        }

        private long WriteMergePass(IEnumerable<MergeResultRecord<T>> pass, Stream outputStream, bool rawReaderSupported)
        {
            using (var rawWriter = rawReaderSupported ? new BinaryRecordWriter<RawRecord>(outputStream) : null)
            using (var writer = rawReaderSupported ? null : new BinaryRecordWriter<T>(outputStream))
            {
                foreach (var record in pass)
                {
                    if (rawWriter == null)
                        writer!.WriteRecord(record.GetValue());
                    else
                        record.WriteRawRecord(rawWriter);
                }
                Interlocked.Add(ref _bytesWritten, writer == null ? rawWriter!.BytesWritten : writer.BytesWritten);
                return writer == null ? rawWriter!.OutputBytes : writer.OutputBytes;
            }
        }

        private MergeResult<T> RunMergePass(IEnumerable<RecordInput> inputs, IComparer<T>? comparer, bool allowRecordReuse, bool rawReaderSupported, bool returnReaders)
        {
            MergePassCount++;
            var mergeQueue = CreateMergeQueue(inputs, comparer, rawReaderSupported, returnReaders, out var readers);

            return new MergeResult<T>(readers, RunMergePassCore(mergeQueue, allowRecordReuse));
        }

        private IEnumerator<MergeResultRecord<T>> RunMergePassCore(PriorityQueue<MergeInput> mergeQueue, bool allowRecordReuse)
        {
            var record = new MergeResultRecord<T>(allowRecordReuse);

            try
            {
                while (mergeQueue.Count > 0)
                {
                    var front = mergeQueue.Peek();
                    front.GetCurrentRecord(record);
                    yield return record;
                    if (front.ReadRecord())
                        mergeQueue.AdjustFirstItem();
                    else
                    {
                        Interlocked.Add(ref _bytesRead, front.BytesRead);
                        front.Dispose();
                        mergeQueue.Dequeue();
                    }
                }
            }
            finally
            {
                while (mergeQueue.Count > 0)
                    mergeQueue.Dequeue().Dispose();
            }
        }

        private static PriorityQueue<MergeInput> CreateMergeQueue(IEnumerable<RecordInput> inputs, IComparer<T>? comparer, bool rawReaderSupported, bool returnReaders, out IRecordReader[]? readers)
        {
            IEnumerable<MergeInput> mergeInputs;
            MergeInputComparer mergeComparer;
            readers = null;
            if (rawReaderSupported)
            {
                var rawComparer = (IRawComparer<T>?)comparer; // Caller makes sure that rawReaderSupported is only true if comparer is a raw comparer or null.
                mergeComparer = new MergeInputComparer(rawComparer ?? RawComparer<T>.CreateComparer());
                mergeInputs = inputs.Select(i => new MergeInput(i.GetRawReader(), i.IsMemoryBased)).Where(i => i.RawRecordReader!.ReadRecord());
                if (returnReaders)
                {
                    mergeInputs = mergeInputs.ToArray();
                    readers = mergeInputs.Select(i => i.RawRecordReader!).ToArray();
                }
            }
            else
            {
                mergeComparer = new MergeInputComparer(comparer ?? Comparer<T>.Default);
                mergeInputs = inputs.Select(i => new MergeInput((RecordReader<T>)i.Reader, i.IsMemoryBased)).Where(i => i.RecordReader!.ReadRecord());
                if (returnReaders)
                {
                    mergeInputs = mergeInputs.ToArray();
                    readers = mergeInputs.Select(i => i.RecordReader!).ToArray();
                }
            }

            return new PriorityQueue<MergeInput>(mergeInputs, mergeComparer);
        }

        private static int GetNumDiskInputsForPass(int pass, int diskInputsRemaining, int maxDiskInputsPerPass)
        {
            /*
             * Taken from Hadoop.
             * Determine the number of segments to merge in a given pass. Assuming more
             * than factor segments, the first pass should attempt to bring the total
             * number of segments - 1 to be divisible by the factor - 1 (each pass
             * takes X segments and produces 1) to minimize the number of merges.
             */
            if (pass > 0 || diskInputsRemaining <= maxDiskInputsPerPass || maxDiskInputsPerPass == 1)
                return Math.Min(diskInputsRemaining, maxDiskInputsPerPass);
            var mod = (diskInputsRemaining - 1) % (maxDiskInputsPerPass - 1);
            if (mod == 0)
                return Math.Min(diskInputsRemaining, maxDiskInputsPerPass); ;
            return mod + 1;
        }
    }
}
