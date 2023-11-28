// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Record reader that reads from multiple other record readers sequentially.
/// </summary>
/// <typeparam name="T">The type of the records.</typeparam>
public sealed class MultiRecordReader<T> : MultiInputRecordReader<T>
    where T : notnull
{
    private RecordReader<T>? _currentReader;
    private int _currentReaderNumber;
    private readonly Stopwatch _timeWaitingStopwatch = new Stopwatch();
    private EventHandler? _hasRecordsChangedHandler;

    /// <summary>
    /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class.
    /// </summary>
    /// <param name="partitions">The partitions that this multi input record reader will read.</param>
    /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
    /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
    /// <param name="bufferSize">The buffer size to use to read input files.</param>
    /// <param name="compressionType">The compression type to us to read input files.</param>
    public MultiRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
        : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
    {
    }

    /// <summary>
    /// Gets the amount of time the record reader spent waiting for input to become available.
    /// </summary>
    public TimeSpan TimeWaiting
    {
        get
        {
            return _timeWaitingStopwatch.Elapsed;
        }
    }

    /// <summary>
    /// Reads a record.
    /// </summary>
    /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
    protected override bool ReadRecordInternal()
    {
        CheckDisposed();
        if (!WaitForReaders())
        {
            return false;
        }

        while (!_currentReader.ReadRecord())
        {
            _currentReader.HasRecordsChanged -= _hasRecordsChangedHandler;
            _currentReader.Dispose();
            _currentReader = null;
            if (!WaitForReaders())
            {
                CurrentRecord = default(T);
                return false;
            }
        }
        CurrentRecord = _currentReader.CurrentRecord;
        return true;
    }

    /// <summary>
    /// Raises the <see cref="MultiInputRecordReader{T}.CurrentPartitionChanged"/> event.
    /// </summary>
    /// <param name="e">The <see cref="System.EventArgs"/> instance containing the event data.</param>
    protected override void OnCurrentPartitionChanged(EventArgs e)
    {
        if (_currentReader != null)
        {
            _currentReader.HasRecordsChanged -= _hasRecordsChangedHandler;
            _currentReader = null;
        }
        _currentReaderNumber = 0;
        base.OnCurrentPartitionChanged(e);
    }

    [MemberNotNullWhen(true, nameof(_currentReader))]
    private bool WaitForReaders()
    {
        if (_currentReader == null)
        {
            var newReaderNumber = _currentReaderNumber + 1;
            if (newReaderNumber > TotalInputCount)
            {
                return false;
            }

            _timeWaitingStopwatch.Start();
            WaitForInputs(newReaderNumber, Timeout.Infinite);
            _timeWaitingStopwatch.Stop();

            _currentReader = (RecordReader<T>)GetInputReader(_currentReaderNumber);
            _currentReaderNumber = newReaderNumber;
            if (_hasRecordsChangedHandler == null)
            {
                _hasRecordsChangedHandler = new EventHandler(_currentReader_HasRecordsChanged);
            }

            _currentReader.HasRecordsChanged += _hasRecordsChangedHandler;
            HasRecords = _currentReader.HasRecords;
        }
        return true;
    }

    /// <summary>
    /// Adds the input.
    /// </summary>
    /// <param name="partitions">The partitions.</param>
    public override void AddInput(IList<RecordInput> partitions)
    {
        base.AddInput(partitions);

        // HACK: Need a different way of handling the events to be able to accurately watch for record availability.
        if (CurrentInputCount == 1)
        {
            HasRecords = true;
        }
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            if (_currentReader != null)
            {
                _currentReader.Dispose();
            }
        }
    }

    void _currentReader_HasRecordsChanged(object? sender, EventArgs e)
    {
        // If the reader has finished, HasRecords will be updated by WaitForReaders (or if we're out of readers, by the RecordReader<T> itself).
        if (!_currentReader!.HasFinished)
        {
            HasRecords = _currentReader.HasRecords;
        }
        // HACK: This implementation is a bit flimsy, as ReadRecord can still block if we reach the end of the current record reader and the next one isn't available yet.
    }
}
