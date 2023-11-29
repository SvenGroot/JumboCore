// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Represents an output record of a merge operation.
/// </summary>
/// <typeparam name="T">The type of the record.</typeparam>
public sealed class MergeResultRecord<T>
    where T : notnull
{
    private T? _record;
    private RawRecord? _rawRecord;
    private MemoryBufferStream? _rawRecordStream;
    private BinaryReader? _rawRecordReader;
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
                _record ??= (T)WritableUtility.GetUninitializedWritable(typeof(T));
                ((IWritable)_record).Read(_rawRecordReader!);
            }
            else
            {
                _record = ValueWriter<T>.ReadValue(_rawRecordReader!);
            }

            _rawRecord = null;
        }
        return _record!;
    }

    /// <summary>
    /// Writes the raw record to the specified writer.
    /// </summary>
    /// <param name="writer">The writer.</param>
    public void WriteRawRecord(RecordWriter<RawRecord> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (_rawRecord == null)
        {
            throw new InvalidOperationException("No raw record stored in this instance.");
        }

        writer.WriteRecord(_rawRecord);
    }

    internal void Reset(T record)
    {
        ArgumentNullException.ThrowIfNull(record);
        _record = record;
        _rawRecord = null;
    }

    internal void Reset(RawRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        _record = default(T);
        _rawRecord = record;
    }
}
