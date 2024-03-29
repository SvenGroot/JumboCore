﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet.Channels;

namespace TaskServerApplication;

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
        ArgumentNullException.ThrowIfNull(taskServer);
        _taskServer = taskServer;
        _indexCache = new PartitionFileIndexCache(maxCacheSize);
        _bufferSize = (int)taskServer.Configuration.FileChannel.ReadBufferSize.Value;
    }

    protected override void HandleConnection(TcpClient client)
    {
        try
        {
            using var stream = client.GetStream();
            using var reader = new BinaryReader(stream);
            using var writer = new BinaryWriter(stream);
            try
            {
                var guidBytes = reader.ReadBytes(16);
                var jobId = new Guid(guidBytes);

                var partitionCount = reader.ReadInt32();
                var partitions = new int[partitionCount];
                for (var x = 0; x < partitionCount; ++x)
                {
                    partitions[x] = reader.ReadInt32();
                }

                var taskCount = reader.ReadInt32();
                var tasks = new string[taskCount];
                for (var x = 0; x < taskCount; ++x)
                {
                    tasks[x] = reader.ReadString();
                }

                var sw = _log.IsDebugEnabled ? Stopwatch.StartNew() : null;
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
        catch (Exception ex)
        {
            _log.Error("An error occurred handling a client connection.", ex);
        }
    }

    private void SendSingleFileOutput(BinaryWriter writer, Guid jobId, int[] partitions, string[] tasks)
    {
        var dir = _taskServer.GetJobDirectory(jobId);
        foreach (var task in tasks)
        {
            var outputFile = FileOutputChannel.CreateChannelFileName(task);
            var path = Path.Combine(dir, outputFile);
            var index = _indexCache.GetIndex(path);
            var totalSize = partitions.Sum(p => index.GetPartitionSize(p, true));
            writer.Write(totalSize);
            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize))
            {
                foreach (var partition in partitions)
                {
                    var entries = index.GetEntriesForPartition(partition);
                    if (entries == null)
                    {
                        writer.Write(0L);
                    }
                    else
                    {
                        var segmentCount = entries.Count();
                        var partitionSize = entries.Sum(e => e.CompressedSize) + sizeof(long) * segmentCount * 2;
                        var uncompressedPartitionSize = entries.Sum(e => e.UncompressedSize);
                        writer.Write(partitionSize);
                        writer.Write(uncompressedPartitionSize);
                        writer.Write(segmentCount);
                        // No need for compressed size because compression is not supported for partition files currently.
                        foreach (var entry in entries)
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
