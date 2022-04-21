using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet
{
    sealed class PartitionMerger<T>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(PartitionMerger<>));

        private readonly List<RecordInput> _memoryInputs = new List<RecordInput>();
        private readonly List<RecordInput> _diskInputs = new List<RecordInput>(); // TODO: Needs to be sorted by file size.
        private readonly MergeRecordReader<T> _reader;
        private readonly int _partitionNumber;
        private readonly IComparer<T> _comparer;
        private readonly string _intermediateFilePrefix;
        private MergeResult<T> _finalPassResult;
        private int _backgroundPassCount;

        public PartitionMerger(MergeRecordReader<T> reader, int partitionNumber, IComparer<T> comparer)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));
            _reader = reader;
            _partitionNumber = partitionNumber;
            _comparer = comparer;
            _intermediateFilePrefix = string.Format(CultureInfo.InvariantCulture, "partition{0}_", partitionNumber);
        }

        public MergeResult<T> FinalPassResult
        {
            get { return _finalPassResult; }
        }

        public float FinalPassProgress
        {
            get { return _finalPassResult == null ? 0.0f : _finalPassResult.Progress; }
        }

        public int PartitionNumber
        {
            get { return _partitionNumber; }
        }

        /// <summary>
        /// NOTE: Only call while _diskInputs is locked!
        /// </summary>
        private bool NeedDiskMergePass
        {
            get { return _diskInputs.Count >= (2 * _reader.MaxDiskInputsPerMergePass - 1); }
        }

        public void AddInput(RecordInput input)
        {
            if (input.IsMemoryBased)
            {
                lock (_memoryInputs)
                    _memoryInputs.Add(input);
            }
            else
            {
                lock (_diskInputs)
                    _diskInputs.Add(input);
            }
        }

        public void PrepareFinalPass(MergeHelper<T> merger)
        {
            lock (_diskInputs)
                lock (_memoryInputs)
                {
                    _finalPassResult = merger.Merge(_diskInputs, _memoryInputs, _reader.MaxDiskInputsPerMergePass, _comparer, _reader.AllowRecordReuse, true, _reader.IntermediateOutputPath, _intermediateFilePrefix, _reader.CompressionType, _reader.BufferSize, _reader.JetConfiguration.FileChannel.EnableChecksum);
                }
        }

        public void RunMemoryPurgePass(MergeHelper<T> merger)
        {
            RecordInput[] passInputs = null;
            lock (_memoryInputs)
            {
                if (_memoryInputs.Count > 0)
                {
                    passInputs = _memoryInputs.ToArray();
                    _memoryInputs.Clear();
                }
            }
            if (passInputs != null)
            {
                ++_backgroundPassCount;
                _log.InfoFormat("Running background pass {0} for partition {1} (memory)", _backgroundPassCount, _partitionNumber);
                string outputFileName = Path.Combine(_reader.IntermediateOutputPath, string.Format(CultureInfo.InvariantCulture, "partition{0}_background_merge{1}.tmp", _partitionNumber, _backgroundPassCount));

                long uncompressedSize = merger.WriteMerge(outputFileName, null, passInputs, _reader.MaxDiskInputsPerMergePass, _comparer, _reader.AllowRecordReuse, _reader.IntermediateOutputPath, _intermediateFilePrefix, _reader.CompressionType, _reader.BufferSize, _reader.JetConfiguration.FileChannel.EnableChecksum);

                _log.Info("Background merge complete");

                lock (_diskInputs)
                    _diskInputs.Add(new FileRecordInput(typeof(BinaryRecordReader<T>), outputFileName, null, uncompressedSize, true, merger.IsUsingRawRecords, 0, _reader.AllowRecordReuse, _reader.BufferSize, _reader.CompressionType));
            }
        }

        public void RunDiskMergePassIfNeeded(MergeHelper<T> merger)
        {
            RecordInput[] passInputs;
            while ((passInputs = GetDiskPassInputs()) != null)
            {
                ++_backgroundPassCount;
                _log.InfoFormat("Running background pass {0} for partition {1} (disk)", _backgroundPassCount, _partitionNumber);

                string outputFileName = Path.Combine(_reader.IntermediateOutputPath, string.Format(CultureInfo.InvariantCulture, "partition{0}_background_merge{1}.tmp", _partitionNumber, _backgroundPassCount));

                long uncompressedSize = merger.WriteMerge(outputFileName, passInputs, null, _reader.MaxDiskInputsPerMergePass, _comparer, _reader.AllowRecordReuse, _reader.IntermediateOutputPath, _intermediateFilePrefix, _reader.CompressionType, _reader.BufferSize, _reader.JetConfiguration.FileChannel.EnableChecksum);

                _log.Info("Background merge complete");

                lock (_diskInputs)
                    _diskInputs.Add(new FileRecordInput(typeof(BinaryRecordReader<T>), outputFileName, null, uncompressedSize, true, merger.IsUsingRawRecords, 0, _reader.AllowRecordReuse, _reader.BufferSize, _reader.CompressionType));
            }
        }

        private RecordInput[] GetDiskPassInputs()
        {
            lock (_diskInputs)
            {
                if (NeedDiskMergePass)
                {
                    RecordInput[] passInputs = _diskInputs.Take(_reader.MaxDiskInputsPerMergePass).ToArray();
                    _diskInputs.RemoveRange(0, _reader.MaxDiskInputsPerMergePass);
                    return passInputs;
                }
            }
            return null;
        }
    }
}
