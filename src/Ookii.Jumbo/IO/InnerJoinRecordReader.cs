// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Record reader that performs a two-way inner equi-join from two sorted input record readers.
    /// </summary>
    /// <typeparam name="TOuter">The type of the records of the outer relation.</typeparam>
    /// <typeparam name="TInner">The type of the records of the inner relation.</typeparam>
    /// <typeparam name="TResult">The type of the result records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Classes inheriting from <see cref="InnerJoinRecordReader{TOuter, TInner, TResult}"/> must specify
    ///   <see cref="InputTypeAttribute"/> attributes with both <typeparamref name="TOuter"/> and <typeparamref name="TInner"/>.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1005:AvoidExcessiveParametersOnGenericTypes")]
    public abstract class InnerJoinRecordReader<TOuter, TInner, TResult> : MultiInputRecordReader<TResult>
        where TResult : notnull, new()
        where TOuter : notnull
        where TInner : notnull
    {
        private RecordReader<TOuter>? _outer;
        private RecordReader<TInner>? _inner;
        private bool _hasTempOuterObject;
        private TOuter? _tempOuterObject;
        private readonly List<TInner> _tempInnerList = new List<TInner>();
        private int _tempInnerListIndex;
        private bool _started;
        private readonly bool _needOuterClone;

        /// <summary>
        /// Initializes a new instance of the <see cref="InnerJoinRecordReader{TOuter, TInner, TResult}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        protected InnerJoinRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
            if (totalInputCount != 2)
                throw new ArgumentOutOfRangeException(nameof(totalInputCount), "InnerJoinRecordReader must have exactly two input readers.");
            if (PartitionCount != 1)
                throw new NotSupportedException("You cannot use multiple partitions with the InnerJoinRecordReader.");
            _needOuterClone = allowRecordReuse && !typeof(TOuter).IsValueType;
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected sealed override bool ReadRecordInternal()
        {
            EnsureStarted();

            TOuter outer;

            while (!_hasTempOuterObject)
            {
                if (_outer.HasFinished || _inner.HasFinished)
                {
                    CurrentRecord = default(TResult);
                    return false;
                }

                outer = _outer.CurrentRecord!;
                var inner = _inner.CurrentRecord!;

                var compareResult = Compare(outer, inner);
                if (compareResult < 0)
                    _outer.ReadRecord();
                else if (compareResult > 0)
                    _inner.ReadRecord();
                else
                {
                    _hasTempOuterObject = true;
                    if (_needOuterClone)
                        _tempOuterObject = (TOuter)((ICloneable)outer).Clone();
                    else
                        _tempOuterObject = outer;
                    if (_outer.ReadRecord())
                    {
                        var nextOuter = _outer.CurrentRecord;
                        if (Compare(nextOuter, inner) == 0)
                        {
                            // There's more than one record in outer that matches inner, which means we need to store the inner records matching this key
                            // so we can compute the cross product.
                            do
                            {
                                if (AllowRecordReuse)
                                    _tempInnerList.Add((TInner)((ICloneable)inner).Clone());
                                else
                                    _tempInnerList.Add(inner);
                                if (_inner.ReadRecord())
                                    inner = _inner.CurrentRecord;
                            } while (!_inner.HasFinished && Compare(outer, inner) == 0);
                        }
                    }
                }
            }

            // We're computing a cross product of an existing matching set of records
            if (!AllowRecordReuse || CurrentRecord == null)
                CurrentRecord = new TResult();
            if (_tempInnerList.Count > 0)
            {
                CurrentRecord = CreateJoinResult(CurrentRecord, _tempOuterObject!, _tempInnerList[_tempInnerListIndex]);
                ++_tempInnerListIndex;
                if (_tempInnerList.Count == _tempInnerListIndex)
                {
                    _tempInnerListIndex = 0;
                    if (!_outer.HasFinished && Compare(_outer.CurrentRecord!, _tempInnerList[0]) == 0)
                    {
                        _hasTempOuterObject = true;
                        if (AllowRecordReuse)
                            _tempOuterObject = (TOuter)((ICloneable)_outer.CurrentRecord!).Clone();
                        else
                            _tempOuterObject = _outer.CurrentRecord;
                        _outer.ReadRecord();
                    }
                    else
                    {
                        _tempOuterObject = default(TOuter);
                        _hasTempOuterObject = false;
                        _tempInnerList.Clear();
                    }
                }
            }
            else
            {
                CurrentRecord = CreateJoinResult(CurrentRecord, _tempOuterObject!, _inner.CurrentRecord!);
                if (!(_inner.ReadRecord() && Compare(_tempOuterObject!, _inner.CurrentRecord) == 0))
                {
                    _tempOuterObject = default(TOuter);
                    _hasTempOuterObject = false;
                }
            }
            return true;
        }

        [MemberNotNull(nameof(_outer))]
        [MemberNotNull(nameof(_inner))]
        private void EnsureStarted()
        {
            if (!_started)
            {
                WaitForInputs(2, Timeout.Infinite);

                _outer!.ReadRecord();
                _inner!.ReadRecord();
                _started = true;
            }

            Debug.Assert(_outer != null && _inner != null);
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
        public override void AddInput(IList<RecordInput> partitions)
        {
            ArgumentNullException.ThrowIfNull(partitions);
            var reader = partitions[0].Reader;
            switch (CurrentInputCount)
            {
            case 0:
                _outer = (RecordReader<TOuter>)reader;
                break;
            case 1:
                _inner = (RecordReader<TInner>)reader;
                _outer!.HasRecordsChanged += new EventHandler(RecordReader_HasRecordsChanged);
                _inner.HasRecordsChanged += new EventHandler(RecordReader_HasRecordsChanged);
                HasRecords = _outer.HasRecords && _inner.HasRecords;
                break;
            default:
                throw new InvalidOperationException();
            }

            // Call this last so that ReadRecordInternal doesn't get signalled before _outer and _inner are assigned.
            base.AddInput(partitions);
        }

        /// <summary>
        /// Assigns additional partitions to this record reader.
        /// </summary>
        /// <param name="newPartitions">The new partitions to assign.</param>
        /// <remarks>
        /// <para>
        ///   New partitions may not be assigned to this record reader, so this method always throws a <see cref="NotSupportedException"/>.
        /// </para>
        /// </remarks>
        public override void AssignAdditionalPartitions(IList<int> newPartitions)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// When implemented in a derived class, compares an object from the outer relation to one from the inner relation based on the join condition.
        /// </summary>
        /// <param name="outer">The outer relation's object.</param>
        /// <param name="inner">The inner relation's object.</param>
        /// <returns>Less than zero if <paramref name="outer"/> is smaller than the <paramref name="inner"/>; greater than zero if <paramref name="outer"/>
        /// is greater than <paramref name="inner"/>; zero if <paramref name="outer"/> and <paramref name="inner"/> are equal based on the join condition.</returns>
        protected abstract int Compare(TOuter outer, TInner inner);

        /// <summary>
        /// When implemented in a derived class, creates an object of type <typeparamref name="TResult"/> that holds the result of the join.
        /// </summary>
        /// <param name="result">An object instance to hold the result.</param>
        /// <param name="outer">The outer relation's object.</param>
        /// <param name="inner">The inner relation's object.</param>
        /// <returns>
        ///   The new result object; this may be the value of <paramref name="result"/>.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   If <see cref="MultiInputRecordReader{TRecord}.AllowRecordReuse"/> is <see langword="true"/>, the value of <paramref name="result"/> will be the same every time this function
        ///   is called. It is therefore important that the implementation of this method always sets all relevant properties of the result object.
        /// </para>
        /// </remarks>
        protected abstract TResult CreateJoinResult(TResult result, TOuter outer, TInner inner);

        private void RecordReader_HasRecordsChanged(object? sender, EventArgs e)
        {
            // Although ReadRecord may not need to wait even if one of them is false (because it's in the middle of computing a cross product)
            // that's too difficult to guarantee so we just check both of them.
            HasRecords = _outer!.HasRecords && _inner!.HasRecords;
        }
    }
}
