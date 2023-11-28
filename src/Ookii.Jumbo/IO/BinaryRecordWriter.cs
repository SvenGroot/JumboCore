// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace Ookii.Jumbo.IO;

/// <summary>
/// A record writer that writes to a file using a binary format based on <see cref="IWritable"/> serialization.
/// </summary>
/// <typeparam name="T">The type of the record to write. Must implement <see cref="IWritable"/> or have an associated <see cref="IValueWriter{T}"/> implementation.</typeparam>
/// <remarks>
/// <para>
///   The data written by this class can be read back by a <see cref="BinaryRecordReader{T}"/> class with the same value for <typeparamref name="T"/>.
///   All records passed to <see cref="RecordWriter{T}.WriteRecord"/> must be <typeparamref name="T"/>; they may not be a type derived
///   from <typeparamref name="T"/>.
/// </para>
/// </remarks>
public class BinaryRecordWriter<T> : StreamRecordWriter<T>
    where T : notnull
{
    private BinaryWriter? _writer;

    /// <summary>
    /// Initializes a new instance of the <see cref="BinaryRecordWriter{T}"/> class.
    /// </summary>
    /// <param name="stream">The stream to write the records to.</param>
    public BinaryRecordWriter(Stream stream)
        : base(stream)
    {
        _writer = new BinaryWriter(stream);
    }

    /// <summary>
    /// Writes the specified record to the stream.
    /// </summary>
    /// <param name="record">The record to write.</param>
    protected override void WriteRecordInternal(T record)
    {
        ArgumentNullException.ThrowIfNull(record);
        CheckDisposed();

        ValueWriter<T>.WriteValue(record, _writer);

        base.WriteRecordInternal(record);
    }

    /// <summary>
    /// Cleans up all resources associated with this <see cref="BinaryRecordWriter{T}"/>.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
    /// to clean up unmanaged resources only.</param>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            if (_writer != null)
            {
                ((IDisposable)_writer).Dispose();
                _writer = null;
            }
        }
    }

    [MemberNotNull(nameof(_writer))]
    private void CheckDisposed()
    {
        if (_writer == null)
        {
            throw new ObjectDisposedException("BinaryRecordWriter");
        }
    }
}
