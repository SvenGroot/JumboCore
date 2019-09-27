// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo;
using System.Net;
using System.IO;
using System.Net.Sockets;
using Ookii.Jumbo.Jet.Channels;
using System.Diagnostics;
using Ookii.Jumbo.Jet;
using System.Globalization;

namespace TaskServerApplication
{
    /// <summary>
    /// A simple file server used by the file channel.
    /// </summary>
    /// <remarks>
    /// The protocol for this is very simple:
    /// - Request port number from the TaskServer.
    /// - Connect.
    /// - Send job ID (byte[])
    /// - Send partition count (int32)
    /// - Send partitions (int32[])
    /// - Send task attempt ID count (int32)
    /// - Send task attempt IDs (string[])
    /// - If multi file output
    ///   - Send output stage ID (string)
    /// - For each task.
    ///   - Server writes total size of all requested partitions
    ///   - For each partition
    ///     - If a failure occurs, the server writes -1 (int64).
    ///     - Server writes partition size (int64, may be 0)
    ///     - Server writes uncompressed file size; if there's no compression this will equal the file size minus segment CRC bytes.
    ///     - Server writes segment count (int32)
    ///     - Server writes partition data
    /// </remarks>
    class FileChannelServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelServer));
        private readonly TaskServer _taskServer;
        private readonly PartitionFileIndexCache _indexCache;
        private readonly int _bufferSize;

        public FileChannelServer(TaskServer taskServer, IPAddress[] localAddresses, int port, int maxConnections, int maxCacheSize)
            : base(localAddresses, port, maxConnections)
        {
            if( taskServer == null )
                throw new ArgumentNullException(nameof(taskServer));
            _taskServer = taskServer;
            _indexCache = new PartitionFileIndexCache(maxCacheSize);
            _bufferSize = (int)taskServer.Configuration.FileChannel.ReadBufferSize.Value;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Exceptions should not crash the server.")]
        protected override void HandleConnection(TcpClient client)
        {
            try
            {
                using NetworkStream stream = client.GetStream();
                using BinaryReader reader = new BinaryReader(stream);
                using BinaryWriter writer = new BinaryWriter(stream);
                try
                {
                    byte[] guidBytes = reader.ReadBytes(16);
                    Guid jobId = new Guid(guidBytes);

                    int partitionCount = reader.ReadInt32();
                    int[] partitions = new int[partitionCount];
                    for (int x = 0; x < partitionCount; ++x)
                        partitions[x] = reader.ReadInt32();

                    int taskCount = reader.ReadInt32();
                    string[] tasks = new string[taskCount];
                    for (int x = 0; x < taskCount; ++x)
                    {
                        tasks[x] = reader.ReadString();
                    }

                    Stopwatch sw = _log.IsDebugEnabled ? Stopwatch.StartNew() : null;
                    SendSingleFileOutput(writer, jobId, partitions, tasks);
                    if (_log.IsDebugEnabled)
                    {
                        sw.Stop();
                        _log.DebugFormat(CultureInfo.InvariantCulture, "Sent tasks {0} partitions {1} to client {2} in {3}ms", tasks.ToDelimitedString(","), partitions.ToDelimitedString(","), client.Client.RemoteEndPoint, sw.ElapsedMilliseconds);
                    }
                }
                catch (Exception)
                {

                    try
                    {
                        writer.Write(-1L);
                    }
                    catch (Exception)
                    {
                    }

                    throw;
                }
            }
            catch( Exception ex )
            {
                _log.Error("An error occurred handling a client connection.", ex);
            }
        }

        private void SendSingleFileOutput(BinaryWriter writer, Guid jobId, int[] partitions, string[] tasks)
        {
            string dir = _taskServer.GetJobDirectory(jobId);
            foreach( string task in tasks )
            {
                string outputFile = FileOutputChannel.CreateChannelFileName(task);
                string path = Path.Combine(dir, outputFile);
                PartitionFileIndex index = _indexCache.GetIndex(path);
                long totalSize = partitions.Sum(p => index.GetPartitionSize(p, true));
                writer.Write(totalSize);
                using( FileStream stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize) )
                {
                    foreach( int partition in partitions )
                    {
                        IEnumerable<PartitionFileIndexEntry> entries = index.GetEntriesForPartition(partition);
                        if( entries == null )
                            writer.Write(0L);
                        else
                        {
                            int segmentCount = entries.Count();
                            long partitionSize = entries.Sum(e => e.CompressedSize) + sizeof(long) * segmentCount * 2;
                            long uncompressedPartitionSize = entries.Sum(e => e.UncompressedSize);
                            writer.Write(partitionSize);
                            writer.Write(uncompressedPartitionSize);
                            writer.Write(segmentCount);
                            // No need for compressed size because compression is not supported for partition files currently.
                            foreach( PartitionFileIndexEntry entry in entries )
                            {
                                writer.Write(entry.CompressedSize);
                                writer.Write(entry.UncompressedSize);
                                stream.Seek(entry.Offset, SeekOrigin.Begin);
                                stream.CopySize(writer.BaseStream, entry.CompressedSize, 65536);
                            }
                        }
                    }
                }
            }
        }
    }
}
