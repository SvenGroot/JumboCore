﻿// Copyright (c) Sven Groot (Ookii.org)
using System.Collections;
using System.Collections.Generic;
using Ookii.Jumbo.Jet.Channels;

namespace TaskServerApplication;

class PartitionFileIndexCache
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(PartitionFileIndexCache));

    private readonly Hashtable _indices; // using Hashtable rather than Dictionary for its concurrency properties.
    private readonly Queue<PartitionFileIndex> _indexQueue;
    private readonly int _maxSize;

    public PartitionFileIndexCache(int maxSize)
    {
        _maxSize = maxSize;
        _indices = new Hashtable(maxSize);
        _indexQueue = new Queue<PartitionFileIndex>(maxSize);
    }

    public PartitionFileIndex GetIndex(string outputFile)
    {
        var index = (PartitionFileIndex)_indices[outputFile];
        if (index == null)
        {
            lock (_indices.SyncRoot)
            {
                index = (PartitionFileIndex)_indices[outputFile];
                if (index == null)
                {
                    _log.DebugFormat("Index cache MISS: {0}", outputFile);
                    if (_indices.Count == _maxSize)
                    {
                        // We cannot safely Dispose the index we removed because some thread may still be using it, so we don't and just wait for the GC to clean up the WaitHandle.
                        var indexToRemove = _indexQueue.Dequeue();
                        _indices.Remove(indexToRemove.OutputFilePath);
                    }
                    index = new PartitionFileIndex(outputFile);
                    _indices.Add(outputFile, index);
                    _indexQueue.Enqueue(index);
                }
            }
        }

        return index;
    }
}
