// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO;

/// <summary>
/// Reads records of the <see cref="GenSortRecord"/> type from a stream.
/// </summary>
public class GenSortRecordReader : StreamRecordReader<GenSortRecord>
{
    private long _position;
    private long _end;

    /// <summary>
    /// Initializes a new instance of the <see cref="GenSortRecordReader"/> class that reads from the specified stream.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to read from.</param>
    public GenSortRecordReader(Stream stream)
        : this(stream, 0, stream.Length, false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="GenSortRecordReader"/> class that reads the specified range of the specified stream.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to read from.</param>
    /// <param name="offset">The offset, in bytes, at which to start reading in the stream.</param>
    /// <param name="size">The number of bytes to read from the stream.</param>
    /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may re-use the same record instance for every
    /// record; <see langword="false"/> if it must create a new instance for every record.</param>
    /// <remarks>
    /// <para>
    ///   If <paramref name="offset"/> is not on a record boundary, the reader will seek ahead to the start of the next record.
    /// </para>
    /// <para>
    ///   The reader will read a whole number of records until the start of the next record falls
    ///   after <paramref name="offset"/> + <paramref name="size"/>. Because of this, the reader can
    ///   read more than <paramref name="size"/> bytes.
    /// </para>
    /// </remarks>
    public GenSortRecordReader(Stream stream, long offset, long size, bool allowRecordReuse)
        : base(stream, offset, size)
    {
        _position = offset;
        _end = offset + size;

        IRecordInputStream? recordInputStream = stream as IRecordInputStream;
        long rem;
        if (recordInputStream != null && (recordInputStream.RecordOptions & RecordStreamOptions.DoNotCrossBoundary) == RecordStreamOptions.DoNotCrossBoundary)
        {
            rem = recordInputStream.OffsetFromBoundary(_position) % GenSortRecord.RecordSize;
        }
        else
        {
            // gensort records are 100 bytes long, making it easy to find the first record.
            rem = _position % GenSortRecord.RecordSize;
        }
        if (rem != 0)
        {
            Stream.Position += GenSortRecord.RecordSize - rem;
            FirstRecordOffset = Stream.Position;
        }
        _position = Stream.Position;

        // Because this reader is only used for GraySort and ValSort, neither of which allow record reuse on the input,
        // we ignore the allowRecordReuse parameter and don't reuse records.
    }

    /// <summary>
    /// Reads a record.
    /// </summary>
    /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
    protected override bool ReadRecordInternal()
    {
        CheckDisposed();

        if (_position >= _end)
        {
            CurrentRecord = null;
            return false;
        }

        GenSortRecord result = new GenSortRecord();
        int bytesRead = Stream.Read(result.RecordBuffer, 0, GenSortRecord.RecordSize);
        if (bytesRead == 0)
        {
            CurrentRecord = null;
            return false;
        }
        else if (bytesRead != GenSortRecord.RecordSize)
        {
            CurrentRecord = null;
            throw new InvalidOperationException("Invalid input file format");
        }

        CurrentRecord = result;

        _position += GenSortRecord.RecordSize;
        return true;
    }
}
