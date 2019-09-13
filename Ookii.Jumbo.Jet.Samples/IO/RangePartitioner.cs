// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs;
using System.IO;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Partitioner that partitions a range into a number of parts.
    /// </summary>
    public sealed class RangePartitioner : Configurable, IPartitioner<GenSortRecord>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RangePartitioner));

        /// <summary>
        /// The name of the partition split file.
        /// </summary>
        public const string SplitFileName = "SplitPoints";

        #region Nested types

        private abstract class TrieNode
        {
            public TrieNode(int depth)
            {
                Depth = depth;
            }

            public int Depth { get; private set; }

            public abstract int GetPartition(byte[] key);
        }

        private sealed class InnerTrieNode : TrieNode
        {
            private TrieNode[] _children = new TrieNode[128]; // GenSort uses 7-bit ASCII so 128 elements is enough.

            public InnerTrieNode(int depth)
                : base(depth)
            {
            }

            public TrieNode this[int index]
            {
                get { return _children[index]; }
                set { _children[index] = value; }
            }

            public override int GetPartition(byte[] key)
            {
                return _children[key[Depth]].GetPartition(key);
            }
        }

        private sealed class LeafTrieNode : TrieNode
        {
            private byte[][] _splitPoints;
            private int _begin;
            private int _end;

            public LeafTrieNode(int depth, byte[][] splitPoints, int begin, int end)
                : base(depth)
            {
                _splitPoints = splitPoints;
                _begin = begin;
                _end = end;
            }

            public override int GetPartition(byte[] key)
            {
                for( int x = _begin; x < _end; ++x )
                {
                    if( GenSortRecord.CompareKeys(key, _splitPoints[x]) < 0 )
                        return x;
                }
                return _end;
            }

            public override string ToString()
            {
                StringBuilder result = new StringBuilder();
                for( int x = _begin; x < _end; ++x )
                {
                    result.Append(_splitPoints[x]);
                    result.Append(";");
                }
                return result.ToString();
            }
        }

        #endregion

        private TrieNode _trie;
        private byte[][] _splitPoints;

        #region IPartitioner<GenSortRecord> Members

        /// <summary>
        /// Gets or sets the number of partitions.
        /// </summary>        
        public int Partitions { get; set; }

        /// <summary>
        /// Gets the partition for the specified value.
        /// </summary>
        /// <param name="value">The value to be partitioned.</param>
        /// <returns>The partition number for the specified value.</returns>
        public int GetPartition(GenSortRecord value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            if( _trie == null )
            {
                ReadPartitionFile();
                _trie = BuildTrie(0, _splitPoints.Length, new byte[] { }, 2);
            }

            return _trie.GetPartition(value.RecordBuffer);
        }

        #endregion

        /// <summary>
        /// Creates a file defining the partitioning split points by sampling the input data.
        /// </summary>
        /// <param name="fileSystemClient">The <see cref="FileSystemClient"/> used to access the DFS.</param>
        /// <param name="partitionFilePath">The path on the DFS where the partitioning data should be stored.</param>
        /// <param name="input">The input of the job.</param>
        /// <param name="partitions">The number of partitions.</param>
        /// <param name="sampleSize">The total number of records to sample.</param>
        public static void CreatePartitionFile(FileSystemClient fileSystemClient, string partitionFilePath, IDataInput input, int partitions, int sampleSize)
        {
            int samples = Math.Min(10, input.TaskInputs.Count);
            int recordsPerSample = sampleSize / samples;
            int sampleStep = input.TaskInputs.Count / samples;
            _log.InfoFormat("Sampling {0} records in {1} samples ({2} records per sample) to create {3} partitions.", sampleSize, samples, recordsPerSample, partitions);

            List<byte[]> sampleData = new List<byte[]>(sampleSize);

            for( int sample = 0; sample < samples; ++sample )
            {
                using( RecordReader<GenSortRecord> reader = (RecordReader<GenSortRecord>)input.CreateRecordReader(input.TaskInputs[sample * sampleStep]) )
                {
                    int records = 0;
                    while( records++ < recordsPerSample && reader.ReadRecord() )
                    {
                        sampleData.Add(reader.CurrentRecord.ExtractKeyBytes());
                    }
                }
            }

            sampleData.Sort(GenSortRecord.CompareKeys);

            _log.InfoFormat("Sampling complete, writing partition file {0}.", partitionFilePath);

            fileSystemClient.Delete(partitionFilePath, false);

            float stepSize = sampleData.Count / (float)partitions;

            using( Stream stream = fileSystemClient.CreateFile(partitionFilePath) )
            {
                for( int x = 1; x < partitions; ++x )
                {
                    stream.Write(sampleData[(int)Math.Round(x * stepSize)], 0, GenSortRecord.KeySize);
                }
            }

            _log.Info("Partition file created.");
        }

        private void ReadPartitionFile()
        {
            List<byte[]> splitPoints = new List<byte[]>();
            string partitionFilePath = Path.Combine(TaskContext.LocalJobDirectory, SplitFileName);
            _log.InfoFormat("Reading local partition split file {0}.", partitionFilePath);
            using( FileStream stream =  File.OpenRead(partitionFilePath) )
            {
                int bytesRead;
                do
                {
                    byte[] key = new byte[GenSortRecord.KeySize];
                    bytesRead = stream.Read(key, 0, GenSortRecord.KeySize);
                    if( bytesRead == GenSortRecord.KeySize )
                    {
                        splitPoints.Add(key);
                    }
                } while( bytesRead == GenSortRecord.KeySize );
            }
            if( splitPoints.Count != Partitions - 1 )
                throw new InvalidOperationException("The partition file is invalid.");
            _splitPoints = splitPoints.ToArray();
        }

        private TrieNode BuildTrie(int begin, int end, byte[] prefix, int maxDepth)
        {
            int depth = prefix.Length;
            if( depth >= maxDepth || begin == end )
                return new LeafTrieNode(depth, _splitPoints, begin, end);

            InnerTrieNode result = new InnerTrieNode(depth);
            int current = begin;
            for( int x = 0; x < 128; ++x )
            {
                byte[] newPrefix = new byte[depth + 1];
                prefix.CopyTo(newPrefix, 0);
                newPrefix[depth] = (byte)(x + 1);
                begin = current;
                while( current < end && GenSortRecord.ComparePartialKeys(_splitPoints[current], newPrefix) < 0 )
                {
                    ++current;
                }
                newPrefix[depth] = (byte)x;
                result[x] = BuildTrie(begin, current, newPrefix, maxDepth);
            }
            return result;
        }
    }
}
