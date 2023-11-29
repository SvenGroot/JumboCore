// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Threading;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

sealed class TcpChannelRecordReader<T> : RecordReader<T>, ITcpChannelRecordReader
    where T : notnull
{
    private readonly BlockingCollection<UnmanagedBufferMemoryStream> _segments = new BlockingCollection<UnmanagedBufferMemoryStream>();
    private readonly bool _allowRecordReuse;
    private bool _disposed;
    private BinaryReader? _currentSegment;
    private readonly T? _record;
    private int _lastSegmentNumber = 0;

    public TcpChannelRecordReader(bool allowRecordReuse)
    {
        // Don't use record reuse for value writers
        if (ValueWriter<T>.Writer == null)
        {
            if (allowRecordReuse)
            {
                _record = (T)WritableUtility.GetUninitializedWritable(typeof(T));
            }

            _allowRecordReuse = true;
        }
    }

    public override float Progress
    {
        get { return _disposed || _segments.IsCompleted ? 1.0f : 0.0f; }
    }

    public void AddSegment(int size, int number, Stream stream)
    {
        CheckDisposed();

        if (++_lastSegmentNumber != number)
        {
            throw new ChannelException(string.Format(CultureInfo.CurrentCulture, "Segment received out of order: expected {0}, got {1}.", _lastSegmentNumber, number));
        }

        // TODO: Maybe we could use the memory storage for this, with file backing if necessary. Would have to check how that works with the merge record reader though
        // TODO: Maybe we should use async I/O for this
        if (size > 0)
        {
            var memoryStream = new UnmanagedBufferMemoryStream(size);
            stream.CopySize(memoryStream, size);
            memoryStream.Position = 0;
            _segments.Add(memoryStream);
            HasRecords = true;
        }
    }

    public void CompleteAdding()
    {
        _segments.CompleteAdding();
    }

    protected override bool ReadRecordInternal()
    {
        CheckDisposed();

        if (_currentSegment == null)
        {
            if (!_segments.TryTake(out var memoryStream, Timeout.Infinite))
            {
                CurrentRecord = default(T);
                return false;
            }
            else
            {
                _currentSegment = new BinaryReader(memoryStream);
            }
        }

        if (_allowRecordReuse)
        {
            ((IWritable)_record!).Read(_currentSegment);
            CurrentRecord = _record;
        }
        else
        {
            CurrentRecord = ValueWriter<T>.ReadValue(_currentSegment);
        }

        if (_currentSegment.BaseStream.Position == _currentSegment.BaseStream.Length)
        {
            _currentSegment.Dispose();
            _currentSegment = null;
            HasRecords = _segments.Count > 0;
        }

        return true;
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (!_disposed)
        {
            _disposed = true;
            if (disposing)
            {
                if (_currentSegment != null)
                {
                    _currentSegment.Dispose();
                }

                _segments.CompleteAdding();
                foreach (var stream in _segments)
                {
                    stream.Dispose();
                }

                _segments.Dispose();
            }
        }
    }

    private void CheckDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(typeof(TcpChannelRecordReader<T>).FullName);
        }
    }
}
