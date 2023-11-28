// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using System.IO;

namespace Ookii.Jumbo.IO;

/// <summary>
/// A record reader that reads files in the record file format.
/// </summary>
/// <typeparam name="T">The type of the records.</typeparam>
/// <remarks>
/// <para>
///   For more information about the format of record files, see <see cref="RecordFileHeader"/>.
/// </para>
/// </remarks>
public class RecordFileReader<T> : StreamRecordReader<T>
    where T : notnull
{
    private BinaryReader _reader;
    private readonly RecordFileHeader _header;
    private readonly byte[] _recordMarker = new byte[RecordFile.RecordMarkerSize];
    private long _lastRecordMarkerPosition;
    private readonly long _end;
    private readonly bool _allowRecordReuse;
    private static readonly IValueWriter<T>? _valueWriter = ValueWriter<T>.Writer;
    private readonly IRecordInputStream? _recordInputStream;

    /// <summary>
    /// Initializes a new instance of the <see cref="RecordFileReader{T}"/> class that reads from the specified stream.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to read from.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
    public RecordFileReader(Stream stream)
        : this(stream, 0, stream.Length, false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RecordFileReader{T}"/> class that reads the specified range of the specified stream,
    /// optionally reusing record instances.
    /// </summary>
    /// <param name="stream">The <see cref="Stream"/> to read from.</param>
    /// <param name="offset">The position in the stream to start reading.</param>
    /// <param name="size">The number of bytes to read from the stream.</param>
    /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may re-use the same record instance for every
    /// record; <see langword="false"/> if it must create a new instance for every record.</param>
    /// <remarks>
    /// <para>
    ///   The <see cref="RecordFileReader{T}"/> will read a whole number of records until the start of the last record marker encountered is
    ///   on or after <paramref name="offset"/> + <paramref name="size"/>.
    /// </para>
    /// <para>
    ///   If <paramref name="offset"/> is greater than the header size, the <see cref="RecordFileReader{T}"/> will seek forward from
    ///   <paramref name="offset "/> until the first record marker found, and read records from there.
    /// </para>
    /// </remarks>
    public RecordFileReader(Stream stream, long offset, long size, bool allowRecordReuse)
        : base(stream, offset, size, false)
    {
        ArgumentNullException.ThrowIfNull(stream);

        _reader = new BinaryReader(stream);
        _header = WritableUtility.GetUninitializedWritable<RecordFileHeader>();
        ((IWritable)_header).Read(_reader);

        if (_header.RecordType != typeof(T))
        {
            throw new InvalidOperationException("The specified record file uses a different record type than the one specified for this reader.");
        }

        _allowRecordReuse = allowRecordReuse;
        _end = offset + size;
        if (offset > stream.Position)
        {
            stream.Position = offset;
            _recordInputStream = stream as IRecordInputStream;
            if (_recordInputStream == null || (_recordInputStream.RecordOptions & RecordStreamOptions.DoNotCrossBoundary) != RecordStreamOptions.DoNotCrossBoundary ||
                _recordInputStream.OffsetFromBoundary(offset) != 0)
            {
                SeekToRecordMarker();
                FirstRecordOffset = stream.Position;
            }
        }
        else
        {
            _lastRecordMarkerPosition = stream.Position - RecordFile.RecordMarkerSize;
        }
        FirstRecordOffset = stream.Position;
    }

    /// <summary>
    /// Gets the header of that was read from the record file.
    /// </summary>
    public RecordFileHeader Header
    {
        get { return _header; }
    }

    /// <summary>
    /// Reads a record.
    /// </summary>
    /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
    protected override bool ReadRecordInternal()
    {
        CheckDisposed();

        while (true)
        {
            if (_lastRecordMarkerPosition >= _end || Stream.Position == Stream.Length || (_recordInputStream != null && _recordInputStream.IsStopped))
            {
                CurrentRecord = default(T);
                return false;
            }

            var recordPrefix = _reader.ReadInt32();
            if (recordPrefix == RecordFile.RecordMarkerPrefix)
            {
                CheckRecordMarker();
            }
            else
            {
                Debug.Assert(recordPrefix == RecordFile.RecordPrefix);

                if (_valueWriter != null)
                {
                    CurrentRecord = _valueWriter.Read(_reader);
                }
                else
                {
                    if (!_allowRecordReuse || CurrentRecord == null)
                    {
                        CurrentRecord = (T)WritableUtility.GetUninitializedWritable(typeof(T));
                    } ((IWritable)CurrentRecord).Read(_reader);
                }
                return true;
            }
        }
    }

    /// <summary>
    /// Cleans up all resources associated with this <see cref="RecordFileReader{T}"/>.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> to clean up both managed and unmanaged resources; <see langword="false"/>
    /// to clean up unmanaged resources only.</param>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
        if (disposing)
        {
            if (_reader != null)
            {
                ((IDisposable)_reader).Dispose();
            }
        }
    }

    private void SeekToRecordMarker()
    {
        _reader.Read(_recordMarker, 0, RecordFile.RecordMarkerSize);

        var fileRecordMarker = _header.RecordMarker;
        for (var x = 0; Stream.Position < _end; ++x)
        {
            int y;
            for (y = 0; y < RecordFile.RecordMarkerSize; ++y)
            {
                if (fileRecordMarker[y] != _recordMarker[(x + y) % RecordFile.RecordMarkerSize])
                {
                    break;
                }
            }
            if (y == RecordFile.RecordMarkerSize)
            {
                _lastRecordMarkerPosition = Stream.Position - RecordFile.RecordMarkerSize;
                return;
            }
            _recordMarker[x % RecordFile.RecordMarkerSize] = _reader.ReadByte();
        }
    }

    private void CheckRecordMarker()
    {
        _reader.Read(_recordMarker, 0, RecordFile.RecordMarkerSize);

        var fileRecordMarker = _header.RecordMarker;
        for (var x = 0; x < RecordFile.RecordMarkerSize; ++x)
        {
            if (fileRecordMarker[x] != _recordMarker[x])
            {
                throw new InvalidOperationException("Invalid record marker in file.");
            }
        }

        _lastRecordMarkerPosition = Stream.Position - RecordFile.RecordMarkerSize;
    }
}
