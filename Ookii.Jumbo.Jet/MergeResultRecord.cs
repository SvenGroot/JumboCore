// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Represents an output record of a merge operation.
    /// </summary>
    /// <typeparam name="T">The type of the record.</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable", Justification = "Memory resources only, no good place to dispose it.")]
    public sealed class MergeResultRecord<T>
    {
        private T _record;
        private RawRecord _rawRecord;
        private MemoryBufferStream _rawRecordStream;
        private BinaryReader _rawRecordReader;
        private readonly bool _allowRecordReuse;

        internal MergeResultRecord(bool allowRecordReuse)
        {
            _allowRecordReuse = allowRecordReuse && ValueWriter<T>.Writer == null;
        }

        /// <summary>
        /// Gets the value of the record.
        /// </summary>
        /// <returns>The value of the record.</returns>
        /// <remarks>
        /// <para>
        ///   If the record was stored in raw form, it is deserialized first.
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Non-trivial code with destructive side-effects.")]
        public T GetValue()
        {
            if (_rawRecord != null)
            {
                if (_rawRecordStream == null)
                {
                    _rawRecordStream = new MemoryBufferStream();
                    _rawRecordReader = new BinaryReader(_rawRecordStream);
                }
                _rawRecordStream.Reset(_rawRecord.Buffer, _rawRecord.Offset, _rawRecord.Count);
                if (_allowRecordReuse) // Implies that the record supports IWritable
                {
                    if (_record == null)
                        _record = (T)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(T));
                    ((IWritable)_record).Read(_rawRecordReader);
                }
                else
                    _record = ValueWriter<T>.ReadValue(_rawRecordReader);
                _rawRecord = null;
            }
            return _record;
        }

        /// <summary>
        /// Writes the raw record to the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public void WriteRawRecord(RecordWriter<RawRecord> writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            if (_rawRecord == null)
                throw new InvalidOperationException("No raw record stored in this instance.");
            writer.WriteRecord(_rawRecord);
        }

        internal void Reset(T record)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));
            _record = record;
            _rawRecord = null;
        }

        internal void Reset(RawRecord record)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));
            _record = default(T);
            _rawRecord = record;
        }
    }
}
