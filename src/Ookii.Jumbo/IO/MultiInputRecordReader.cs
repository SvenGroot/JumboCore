// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides methods for working with multi-input record readers.
    /// </summary>
    public static class MultiInputRecordReader
    {
        /// <summary>
        /// Gets the accepted input types for a multi-input record reader.
        /// </summary>
        /// <param name="type">The type of multi-input record reader.</param>
        /// <returns>A list of accepted types.</returns>
        public static IEnumerable<Type> GetAcceptedInputTypes(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            var baseType = type.FindGenericBaseType(typeof(MultiInputRecordReader<>), true)!;
            var attributes = type.GetCustomAttributes<InputTypeAttribute>();
            if (!attributes.Any())
            {
                return baseType.GetGenericArguments().Take(1);
            }
            else
            {
                return attributes.Select(a => a.AcceptedType);
            }
        }
    }

    /// <summary>
    /// Base class for record readers that combine multiple inputs.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Depending on the type of record reader, the records of the input record readers might not
    ///   need to read records of type <typeparamref name="T"/>.
    /// </para>
    /// <para>
    ///   If you accept inputs of types other than <typeparamref name="T"/>, you must specify that using the <see cref="InputTypeAttribute"/>.
    /// </para>
    /// <para>
    ///   The initial value of <see cref="RecordReader{T}.HasRecords"/> will be <see langword="false"/>. It is up to the deriving class
    ///   to set it to <see langword="true"/> when appropriate.
    /// </para>
    /// <note>
    ///   While the <see cref="AddInput"/>, <see cref="WaitForInputs"/> 
    ///   and <see cref="GetInputReader(int)"/> methods are thread safe, no other methods of this class are guaranteed to be thread
    ///   safe, and derived classes are not required to make <see cref="RecordReader{T}.ReadRecordInternal"/> thread safe.
    ///   Essentially, you may have only one thread reading from the <see cref="MultiInputRecordReader{T}"/>, while one or
    ///   more other threads add inputs to it.
    /// </note>
    /// </remarks>
    public abstract class MultiInputRecordReader<T> : RecordReader<T>, IMultiInputRecordReader
        where T : notnull
    {
        #region Nested types

        private sealed class Partition : IDisposable
        {
            private readonly int _partitionNumber;
            private readonly List<RecordInput> _inputs;
            private volatile int _firstNonMemoryIndex;
            private long _inputBytes = -1;
            private long _bytesRead = -1;

            public Partition(int partitionNumber, int totalInputCount)
            {
                _partitionNumber = partitionNumber;
                _inputs = new List<RecordInput>(totalInputCount);
            }

            public int PartitionNumber
            {
                get { return _partitionNumber; }
            }

            public float ProgressSum
            {
                get
                {
                    return (from input in _inputs
                            select input.Progress).Sum();
                }
            }

            public long InputBytesSum
            {
                get
                {
                    if (_inputBytes >= 0)
                        return _inputBytes;

                    return (from input in _inputs
                            where input.IsReaderCreated
                            select input.Reader.InputBytes).Sum();
                }
            }

            public long BytesReadSum
            {
                get
                {
                    if (_bytesRead >= 0)
                        return _bytesRead;

                    return (from input in _inputs
                            where input.IsReaderCreated
                            select input.Reader.BytesRead).Sum();
                }
            }

            public int InputCount
            {
                get
                {
                    return _inputs.Count;
                }
            }

            public void AddInput(RecordInput input)
            {
                if (input.IsMemoryBased)
                {
                    _inputs.Insert(_firstNonMemoryIndex, input);
                    ++_firstNonMemoryIndex;
                }
                else
                {
                    _inputs.Add(input);
                }
            }

            public RecordInput? GetInput(int index, bool memoryOnly)
            {
                // Inputs that have been retrieved may not be moved; adjusting the _firstNonMemoryIndex field will make sure they won't be.
                var result = _inputs[index];
                if (memoryOnly && !result.IsMemoryBased)
                    return null;
                if (index >= _firstNonMemoryIndex)
                    _firstNonMemoryIndex = index + 1;
                return result;
            }

            public bool Exists(Predicate<RecordInput> match)
            {
                return _inputs.Exists(match);
            }

            public void Dispose()
            {
                if (_inputBytes == -1)
                    _inputBytes = InputBytesSum;
                if (_bytesRead == -1)
                    _bytesRead = BytesReadSum;
                foreach (var input in _inputs)
                    input.Dispose();
            }
        }

        #endregion

        private bool _disposed;
        private readonly List<Partition> _partitions = new List<Partition>();
        private readonly Dictionary<int, Partition> _partitionsByNumber = new Dictionary<int, Partition>(); // lock _partitions to access this member.
        private int _currentPartitionIndex;
        private int _firstActivePartitionIndex;

        /// <summary>
        /// Event raised when the value of the <see cref="CurrentPartition"/> property changes.
        /// </summary>
        public event EventHandler? CurrentPartitionChanged;

        /// <summary>
        /// Event raised when the value of the <see cref="CurrentPartition"/> property is about to change.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If you set <see cref="CancelEventArgs.Cancel"/> to <see langword="true"/> in the handler
        ///   for this event, the <see cref="NextPartition"/> method will skip the indicated partition
        ///   and move to the next one.
        /// </para>
        /// <para>
        ///   The <see cref="CurrentPartitionChanged"/> event will not be raised for partitions
        ///   that were skipped in this fashion.
        /// </para>
        /// </remarks>
        public event EventHandler<CurrentPartitionChangingEventArgs>? CurrentPartitionChanging;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiInputRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        protected MultiInputRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(false)
        {
            ArgumentNullException.ThrowIfNull(partitions);
            if (totalInputCount < 1)
                throw new ArgumentOutOfRangeException(nameof(totalInputCount), "Multi input record reader must have at least one input.");
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be larger than zero.");

            foreach (var partitionNumber in partitions)
            {
                var partition = new Partition(partitionNumber, totalInputCount);
                _partitions.Add(partition);
                _partitionsByNumber.Add(partitionNumber, partition);
            }

            TotalInputCount = totalInputCount;
            AllowRecordReuse = allowRecordReuse;
            BufferSize = bufferSize;
            CompressionType = compressionType;
        }

        /// <summary>
        /// Gets the total number of inputs readers that this record reader will have.
        /// </summary>
        public int TotalInputCount { get; private set; }

        /// <summary>
        /// Gets a value that indicates that this record reader is allowed to reuse record instances.
        /// </summary>
        public bool AllowRecordReuse { get; private set; }

        /// <summary>
        /// Gets the buffer size to use to read input files.
        /// </summary>
        public int BufferSize { get; private set; }

        /// <summary>
        /// Gets the type of compression to use to read input files.
        /// </summary>
        public CompressionType CompressionType { get; private set; }

        /// <summary>
        /// Gets the combined progress of the record readers.
        /// </summary>
        /// <value>A value between 0 and 1 that indicates the overall progress of the <see cref="MultiInputRecordReader{T}"/>.</value>
        public override float Progress
        {
            get
            {
                lock (_partitions)
                {
                    if (_partitions.Count == 0) // prevent division by zero.
                        return 0;

                    return (from partition in _partitions
                            select partition.ProgressSum).Sum() / (TotalInputCount * _partitions.Count);
                }
            }
        }

        /// <summary>
        /// Gets the size of the records before deserialization of all record readers.
        /// </summary>
        /// <value>
        /// The size of the records before deserialization, or 0 if the records were not read from a serialized source.
        /// </value>
        public override long InputBytes
        {
            get
            {
                lock (_partitions)
                {
                    return (from partition in _partitions
                            select partition.InputBytesSum).Sum();
                }
            }
        }

        /// <summary>
        /// Gets the actual number of bytes read from the input.
        /// </summary>
        /// <value>
        /// The number of bytes read from the input.
        /// </value>
        /// <remarks>
        /// <para>
        ///   This is the value of <see cref="InputBytes"/>, adjusted for compression (if applicable) and including any additional data read by the record reader (if any).
        /// </para>
        /// </remarks>
        public override long BytesRead
        {
            get
            {
                lock (_partitions)
                {
                    return (from partition in _partitions
                            select partition.BytesReadSum).Sum();
                }
            }
        }

        /// <summary>
        /// Gets the current number of inputs that have been added to the <see cref="MultiInputRecordReader{T}"/> for the currently active set of partitions.
        /// </summary>
        public int CurrentInputCount
        {
            get
            {
                lock (_partitions)
                {
                    return _partitions.Count == 0 ? 0 : _partitions[_firstActivePartitionIndex].InputCount;
                }
            }
        }

        /// <summary>
        /// Gets the partition numbers assigned to this reader.
        /// </summary>
        /// <value>The partition numbers assigned to this reader.</value>
        public IList<int> PartitionNumbers
        {
            get
            {
                lock (_partitions)
                {
                    return (from p in _partitions
                            select p.PartitionNumber).ToList();
                }
            }
        }

        /// <summary>
        /// Gets the number of partitions assigned to this reader.
        /// </summary>
        /// <value>The number of partitions assigned to this reader.</value>
        public int PartitionCount
        {
            get
            {
                lock (_partitions)
                {
                    return _partitions.Count;
                }
            }
        }

        /// <summary>
        /// Gets or sets the partition that calls to <see cref="RecordReader{T}.ReadRecord"/> should return records for.
        /// </summary>
        /// <value>The current partition.</value>
        /// <para>
        /// The current partition determines which partition the <see cref="RecordReader{T}.ReadRecord"/> function should return records for.
        /// Deriving classes should use this when implementing <see cref="RecordReader{T}.ReadRecordInternal"/>.
        /// </para>
        public int CurrentPartition
        {
            get { return _partitions[_currentPartitionIndex].PartitionNumber; }
        }

        /// <summary>
        /// Gets a value that indicates whether the object has been disposed.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this instance is disposed; otherwise, <see langword="false"/>.
        /// </value>
        protected bool IsDisposed
        {
            get { return _disposed; }
        }

        /// <summary>
        /// Moves the current partition to the next partition.
        /// </summary>
        /// <returns><see langword="true"/> if the current partition was moved to the next partition; <see langword="false"/> if there were no more partitions.</returns>
        /// <remarks>
        /// <para>
        ///   The current partition determines which partition the <see cref="RecordReader{T}.ReadRecord"/> function should return records for.
        ///   Deriving classes should use this when implementing <see cref="RecordReader{T}.ReadRecordInternal"/>.
        /// </para>
        /// </remarks>
        public bool NextPartition()
        {
            lock (_partitions)
            {
                var newPartitionIndex = _currentPartitionIndex + 1;
                _partitions[_currentPartitionIndex].Dispose();
                while (newPartitionIndex < _partitions.Count)
                {
                    if (_currentPartitionIndex < _partitions.Count - 1)
                    {
                        var e = new CurrentPartitionChangingEventArgs(_partitions[newPartitionIndex].PartitionNumber);
                        OnCurrentPartitionChanging(e);
                        if (e.Cancel)
                        {
                            _partitions[newPartitionIndex].Dispose();
                            ++newPartitionIndex;
                        }
                        else
                        {
                            _currentPartitionIndex = newPartitionIndex;
                            OnCurrentPartitionChanged(EventArgs.Empty);
                            return true;
                        }
                    }
                    else
                        return false;
                }

                // Set the current partition index so that if AssignAdditionalPartitions is called, NextPartition will work right after that.
                // But don't raise the CurrentPartitionChanged event because we're not going to process that partition (and have already disposed it).
                _currentPartitionIndex = _partitions.Count - 1;
                return false;
            }
        }

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input, in the same order as the partition list provided to the constructor.</param>
        /// <remarks>
        /// <para>
        ///   Which partitions a multi input record reader is responsible for is specified when that reader is created or
        ///   when <see cref="AssignAdditionalPartitions"/> is called. All calls to <see cref="AddInput"/> must specify those
        ///   exact same partitions, in the same order.
        /// </para>
        /// <para>
        ///   If you override this method, you must call the base class implementation.
        /// </para>
        /// </remarks>
        public virtual void AddInput(IList<RecordInput> partitions)
        {
            ArgumentNullException.ThrowIfNull(partitions);

            lock (_partitions)
            {
                if (partitions.Count != _partitions.Count - _firstActivePartitionIndex)
                    throw new ArgumentException("Incorrect number of partitions.");
                if (CurrentInputCount >= TotalInputCount)
                    throw new InvalidOperationException("The merge task input already has all inputs.");

                for (var x = 0; x < partitions.Count; ++x)
                {
                    var input = partitions[x];
                    _partitions[_firstActivePartitionIndex + x].AddInput(input);
                }

                Monitor.PulseAll(_partitions);
            }
        }

        /// <summary>
        /// Assigns additional partitions to this record reader.
        /// </summary>
        /// <param name="newPartitions">The new partitions to assign.</param>
        /// <remarks>
        /// <para>
        ///   New partitions may only be assigned after all inputs for the existing partitions have been received.
        /// </para>
        /// </remarks>
        public virtual void AssignAdditionalPartitions(IList<int> newPartitions)
        {
            ArgumentNullException.ThrowIfNull(newPartitions);
            if (newPartitions.Count == 0)
                throw new ArgumentException("The list of new partitions is empty.", nameof(newPartitions));

            lock (_partitions)
            {
                if (_partitions.Count > 0 && _partitions[_firstActivePartitionIndex].InputCount != TotalInputCount)
                    throw new InvalidOperationException("You cannot assign new partitions to a record reader until the currently assigned partitions have all their inputs.");

                _firstActivePartitionIndex = _partitions.Count;
                foreach (var partitionNumber in newPartitions)
                {
                    var partition = new Partition(partitionNumber, TotalInputCount);
                    _partitions.Add(partition);
                    _partitionsByNumber.Add(partitionNumber, partition);
                }
            }
        }

        /// <summary>
        /// Waits until the specified number of inputs becomes available for all currently active partitions.
        /// </summary>
        /// <param name="inputCount">The number of inputs to wait for.</param>
        /// <param name="timeout">The maximum amount of time to wait, in milliseconds, or <see cref="System.Threading.Timeout.Infinite"/> to wait indefinitely.</param>
        /// <returns><see langword="true"/> if a new input is available; <see langword="false"/> if the timeout expired.</returns>
        /// <remarks>
        /// <para>
        ///   This function will wait for the specified number of inputs on the last group of partitions assigned to the reader via
        ///   <see cref="AssignAdditionalPartitions"/>.
        /// </para>
        /// </remarks>
        protected bool WaitForInputs(int inputCount, int timeout)
        {
            CheckDisposed();
            if (inputCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(inputCount), "inputCount must be greater than zero.");
            if (inputCount > TotalInputCount)
                inputCount = TotalInputCount;
            Stopwatch? sw = null;
            if (timeout > 0)
                sw = Stopwatch.StartNew();
            lock (_partitions)
            {
                while (_partitions[_firstActivePartitionIndex].InputCount < inputCount)
                {
                    var timeoutRemaining = Timeout.Infinite;
                    if (timeout == 0)
                        return false;
                    if (timeout > 0)
                    {
                        timeoutRemaining = (int)(timeout - sw!.ElapsedMilliseconds);
                        if (timeoutRemaining <= 0)
                            return false;
                    }
                    if (!Monitor.Wait(_partitions, timeoutRemaining))
                        return false;
                }
                return true;
            }
        }

        /// <summary>
        /// Gets the record reader for the specified input of the current partition.
        /// </summary>
        /// <param name="index">The index of the input.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        protected IRecordReader GetInputReader(int index)
        {
            lock (_partitions)
            {
                return _partitions[_currentPartitionIndex].GetInput(index, false)!.Reader;
            }
        }

        /// <summary>
        /// Returns the record reader for the specified partition and input.
        /// </summary>
        /// <param name="partition">The partition of the reader to return.</param>
        /// <param name="index">The index of the record reader to return.</param>
        /// <returns>An instance of a class implementing <see cref="IRecordReader"/> for the specified input.</returns>
        /// <remarks>
        /// <para>
        ///   Once a call to <see cref="GetInputReader(int,int)"/> has returned an input, subsequent
        ///   calls with the same <paramref name="partition"/> and <paramref name="index"/> are guaranteed to return the same value.
        /// </para>
        /// <para>
        ///   Two calls to <see cref="GetInputReader(int,int)"/> with the same <paramref name="index"/> but a different <paramref name="partition"/>
        ///   aren't guaranteed to return inputs from the same source.
        /// </para>
        /// </remarks>
        protected IRecordReader? GetInputReader(int partition, int index)
        {
            return GetInputReader(partition, index, false);
        }

        /// <summary>
        /// Returns the record reader for the specified partition and input.
        /// </summary>
        /// <param name="partition">The partition of the reader to return.</param>
        /// <param name="index">The index of the record reader to return.</param>
        /// <param name="memoryOnly"><see langword="true"/> to return only inputs that are stored in memory; otherwise, <see langword="false"/>.</param>
        /// <returns>
        /// An instance of a class implementing <see cref="IRecordReader"/> for the specified input, or <see langword="null"/> if <paramref name="memoryOnly"/>
        /// was <see langword="true"/> and the input was not stored in memory.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   Because the <see cref="MultiInputRecordReader{T}"/> tries to keep the inputs that in memory in front of the inputs that aren't, if
        ///   a call to <see cref="GetInputReader(int,int,bool)"/> returned <see langword="null"/>, subsequent calls with the same <paramref name="partition"/>
        ///   and <paramref name="index"/> might not return <see langword="null"/>.
        /// </para>
        /// <para>
        ///   Once a call to <see cref="GetInputReader(int,int,bool)"/> has returned a value other than <see langword="null"/>, subsequent
        ///   calls with the same <paramref name="partition"/> and <paramref name="index"/> are guaranteed to return the same value.
        /// </para>
        /// <para>
        ///   Two calls to <see cref="GetInputReader(int,int,bool)"/> with the same <paramref name="index"/> but a different <paramref name="partition"/>
        ///   aren't guaranteed to return inputs from the same source.
        /// </para>
        /// </remarks>
        protected IRecordReader? GetInputReader(int partition, int index, bool memoryOnly)
        {
            lock (_partitions)
            {
                var input = _partitionsByNumber[partition].GetInput(index, memoryOnly);
                return input == null ? null : input.Reader;
            }
        }

        /// <summary>
        /// Returns the specified input.
        /// </summary>
        /// <param name="partition">The partition of the input to return.</param>
        /// <param name="index">The index of the record input to return.</param>
        /// <param name="memoryOnly"><see langword="true"/> to return only inputs that are stored in memory; otherwise, <see langword="false"/>.</param>
        /// <returns>
        /// The <see cref="RecordInput"/> instance for the input, or <see langword="null"/> if <paramref name="memoryOnly"/>
        /// was <see langword="true"/> and the input was not stored in memory.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   Because the <see cref="MultiInputRecordReader{T}"/> tries to keep the inputs that in memory in front of the inputs that aren't, if
        ///   a call to <see cref="GetInput(int,int,bool)"/> returned <see langword="null"/>, subsequent calls with the same <paramref name="partition"/>
        ///   and <paramref name="index"/> might not return <see langword="null"/>.
        /// </para>
        /// <para>
        ///   Once a call to <see cref="GetInput(int,int,bool)"/> has returned a value other than <see langword="null"/>, subsequent
        ///   calls with the same <paramref name="partition"/> and <paramref name="index"/> are guaranteed to return the same value.
        /// </para>
        /// <para>
        ///   Two calls to <see cref="GetInput(int,int,bool)"/> with the same <paramref name="index"/> but a different <paramref name="partition"/>
        ///   aren't guaranteed to return inputs from the same source.
        /// </para>
        /// </remarks>
        protected RecordInput? GetInput(int partition, int index, bool memoryOnly)
        {
            lock (_partitions)
            {
                return _partitionsByNumber[partition].GetInput(index, memoryOnly);
            }
        }

        /// <summary>
        /// Cleans up all resources associated with this <see cref="MultiInputRecordReader{T}"/>.
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
        /// to clean up unmanaged resources only.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if (!_disposed)
                {
                    _disposed = true;
                    if (disposing)
                    {
                        lock (_partitions)
                        {
                            foreach (var partition in _partitions)
                            {
                                partition.Dispose();
                            }
                            _partitions.Clear();
                            _partitionsByNumber.Clear();
                        }
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Raises the <see cref="CurrentPartitionChanged"/> event.
        /// </summary>
        /// <param name="e">The data for the event.</param>
        protected virtual void OnCurrentPartitionChanged(EventArgs e)
        {
            var handler = CurrentPartitionChanged;
            if (handler != null)
                handler(this, e);
        }

        /// <summary>
        /// Raises the <see cref="CurrentPartitionChanging"/> event.
        /// </summary>
        /// <param name="e">The data for the event.</param>
        protected virtual void OnCurrentPartitionChanging(CurrentPartitionChangingEventArgs e)
        {
            var handler = CurrentPartitionChanging;
            if (handler != null)
                handler(this, e);
        }

        /// <summary>
        /// Throws a <see cref="ObjectDisposedException"/> if the object has been disposed.
        /// </summary>
        protected void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(GetType().FullName);
        }
    }
}
