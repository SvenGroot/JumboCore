// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Threading;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Multi input record reader that reads from all currently available inputs in a round robin fashion.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   This class does not wait for all inputs to be available. Whatever inputs are available when <see cref="RecordReader{T}.ReadRecord"/>
    ///   is called will be used in the list. Inputs where <see cref="IRecordReader.HasRecords"/> is <see langword="false"/>
    ///   will be skipped.
    /// </para>
    /// </remarks>
    public sealed class RoundRobinMultiInputRecordReader<T> : MultiInputRecordReader<T>
    {
        private readonly List<RecordReader<T>> _readers = new List<RecordReader<T>>();
        private int _previousInputsAvailable;
        private int _currentReader = -1;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        public RoundRobinMultiInputRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            while (true)
            {
                if (_readers.Count == 0)
                {
                    if (_previousInputsAvailable == TotalInputCount)
                    {
                        CurrentRecord = default(T);
                        return false;
                    }
                    else
                        WaitForInputs(_previousInputsAvailable + 1, Timeout.Infinite);
                }

                var inputsAvailable = CurrentInputCount;
                if (inputsAvailable > _previousInputsAvailable)
                {
                    for (var x = _previousInputsAvailable; x < inputsAvailable; ++x)
                        _readers.Add((RecordReader<T>)GetInputReader(x));
                    _previousInputsAvailable = inputsAvailable;
                    if (_currentReader == -1)
                        _currentReader = _readers.Count - 1;
                }

                var nextReader = (_currentReader + 1) % _readers.Count;

                while (nextReader != _currentReader)
                {
                    var reader = _readers[nextReader];
                    if (reader.HasRecords)
                    {
                        if (ReadRecordFromReader(nextReader, reader))
                            return true;
                        else if (nextReader >= _readers.Count)
                            nextReader = _readers.Count - 1;
                    }
                    else
                        nextReader = (nextReader + 1) % _readers.Count;
                }

                // If we got here, we didn't find any record to return.
                // We're going to go through the list again, this time ignoring RecordsAvailable.
                nextReader = (_currentReader + 1) % _readers.Count;
                while (_readers.Count > 0)
                {
                    var reader = _readers[nextReader];
                    if (ReadRecordFromReader(nextReader, reader))
                        return true;
                    else
                    {
                        if (nextReader >= _readers.Count)
                            nextReader = _readers.Count - 1;
                    }
                }
            }
        }

        /// <summary>
        /// Raises the <see cref="MultiInputRecordReader{T}.CurrentPartitionChanged"/> event.
        /// </summary>
        /// <param name="e">The <see cref="System.EventArgs"/> instance containing the event data.</param>
        protected override void OnCurrentPartitionChanged(EventArgs e)
        {
            _currentReader = -1;
            _readers.Clear();
            _previousInputsAvailable = 0;
        }

        private bool ReadRecordFromReader(int index, RecordReader<T> reader)
        {
            try
            {
                if (reader.ReadRecord())
                {
                    _currentReader = index;
                    CurrentRecord = reader.CurrentRecord;
                    return true;
                }
                else
                {
                    _readers.RemoveAt(index);
                    if (index < _currentReader)
                        --_currentReader;
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw new ChildReaderException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Error reading from source {0}.", reader.SourceName), ex);
            }
        }
    }
}
