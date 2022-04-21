// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A record reader that reads from a list. Mainly for test purposes.
    /// </summary>
    /// <typeparam name="T">The type of record.</typeparam>
    public class EnumerableRecordReader<T> : RecordReader<T>
    {
        private IEnumerator<T> _enumerator;
        private int _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumerableRecordReader{T}"/> class.
        /// </summary>
        /// <param name="source">The list to read from.</param>
        /// <param name="count">The number of items in the list, or zero if this is unknown.</param>
        /// <remarks>
        /// <para>
        ///   If <paramref name="count"/> is zero, <see cref="Progress"/> will return 0 until the reader has finished.
        /// </para>
        /// </remarks>
        public EnumerableRecordReader(IEnumerable<T> source, int count)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            _enumerator = source.GetEnumerator();
            _count = count;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumerableRecordReader&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="source">The list to read from.</param>
        public EnumerableRecordReader(IList<T> source)
            : this(source, source == null ? 0 : source.Count)
        {

        }

        /// <summary>
        /// Gets the progress of the reader.
        /// </summary>
        public override float Progress
        {
            get
            {
                if (_count == 0)
                    return HasFinished ? 1.0f : 0.0f;
                else
                    return (float)RecordsRead / (float)_count;
            }
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            if (_enumerator.MoveNext())
            {
                CurrentRecord = _enumerator.Current;
                return true;
            }
            else
            {
                CurrentRecord = default(T);
                return false;
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
                if (_enumerator != null)
                {
                    _enumerator.Dispose();
                }
            }
        }
    }
}
