// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Index data for partition files. For Jumbo internal use only.
    /// </summary>
    public sealed class PartitionFileIndex : IDisposable
    {
        private ManualResetEvent _loadCompleteEvent = new ManualResetEvent(false);
        private Exception _loadException = null;
        private List<PartitionFileIndexEntry>[] _index;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PartitionFileIndex"/> class.
        /// </summary>
        /// <param name="outputFilePath">The output file path.</param>
        public PartitionFileIndex(string outputFilePath)
        {
            OutputFilePath = outputFilePath;
            ThreadPool.QueueUserWorkItem(LoadIndex, outputFilePath + ".index");
        }

        /// <summary>
        /// Gets the output file path.
        /// </summary>
        /// <value>The output file path.</value>
        public string OutputFilePath { get; private set; }

        /// <summary>
        /// Gets the index entries for the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <returns>A list of partition index entries, or <see langword="null"/> if the file contains no data for this partition.</returns>
        public IEnumerable<PartitionFileIndexEntry> GetEntriesForPartition(int partition)
        {
            WaitUntilLoaded();
            if (partition < 1 || partition > _index.Length)
                throw new ArgumentOutOfRangeException(nameof(partition));
            return _index[partition - 1];
        }

        /// <summary>
        /// Gets the size of the specified partition.
        /// </summary>
        /// <param name="partition">The partition.</param>
        /// <param name="includeSegmentHeader">If set to <see langword="true" />, include an additional 8 bytes for each segment.</param>
        /// <returns>The size of the partition.</returns>
        public long GetPartitionSize(int partition, bool includeSegmentHeader)
        {
            WaitUntilLoaded();
            if (partition < 1 || partition > _index.Length)
                throw new ArgumentOutOfRangeException(nameof(partition));
            List<PartitionFileIndexEntry> index = _index[partition - 1];
            long result = 0;
            if (index != null)
            {
                result = index.Sum(e => e.CompressedSize);
                if (includeSegmentHeader)
                    result += index.Count * sizeof(long) * 2;
            }
            return result;
        }

        private void WaitUntilLoaded()
        {
            _loadCompleteEvent.WaitOne();
            if (_loadException != null)
                throw new TargetInvocationException(_loadException);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void LoadIndex(object state)
        {
            try
            {
                string indexFilePath = (string)state;
                using (FileStream stream = File.OpenRead(indexFilePath))
                using (BinaryRecordReader<PartitionFileIndexEntry> reader = new BinaryRecordReader<PartitionFileIndexEntry>(stream, false))
                {
                    foreach (PartitionFileIndexEntry entry in reader.EnumerateRecords())
                    {
                        if (_index == null)
                            _index = new List<PartitionFileIndexEntry>[entry.Partition]; // First entry isn't a real entry but gives us the total number of partitions.
                        else
                        {
                            List<PartitionFileIndexEntry> partition = _index[entry.Partition];
                            if (partition == null)
                            {
                                partition = new List<PartitionFileIndexEntry>(1);
                                _index[entry.Partition] = partition;
                            }
                            partition.Add(entry);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _loadException = ex;
            }
            _loadCompleteEvent.Set();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                ((IDisposable)_loadCompleteEvent).Dispose();
            }
            GC.SuppressFinalize(this);
        }
    }
}
