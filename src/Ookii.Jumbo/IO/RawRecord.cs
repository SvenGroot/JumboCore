// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Represents a serialized representation of a record.
/// </summary>
public sealed class RawRecord : IWritable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RawRecord"/> class.
    /// </summary>
    public RawRecord()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RawRecord"/> class.
    /// </summary>
    /// <param name="buffer">The buffer containing the record.</param>
    /// <param name="offset">The offset in <paramref name="buffer"/> at which the record starts.</param>
    /// <param name="count">The number of bytes in <paramref name="buffer"/> for the record.</param>
    /// <remarks>
    /// <para>
    ///   The value of <paramref name="buffer"/> will be stored in this instance and returned by the <see cref="Buffer"/> property.
    ///   Any changes made to the contents of the buffer after calling <see cref="Reset"/> will be reflected in this instance.
    /// </para>
    /// <note>
    ///   Calling the <see cref="Read"/> method may overwrite the contents of the current buffer at offset zero, or allocate a new one if that buffer
    ///   is not big enough.
    /// </note>
    /// </remarks>
    public RawRecord(byte[] buffer, int offset, int count)
    {
        Reset(buffer, offset, count);
    }

    /// <summary>
    /// Gets the buffer containing the raw record.
    /// </summary>
    public byte[]? Buffer { get; private set; }
    /// <summary>
    /// Gets the offset in <see cref="Buffer"/> at which the record starts.
    /// </summary>
    public int Offset { get; private set; }
    /// <summary>
    /// Gets the number of bytes in <see cref="Buffer"/> for the record.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    /// Resets the this instance of the <see cref="RawRecord"/> class for a new record.
    /// </summary>
    /// <param name="buffer">The buffer containing the record.</param>
    /// <param name="offset">The offset in <paramref name="buffer"/> at which the record starts.</param>
    /// <param name="count">The number of bytes in <paramref name="buffer"/> for the record.</param>
    /// <remarks>
    /// <para>
    ///   The value of <paramref name="buffer"/> will be stored in this instance and returned by the <see cref="Buffer"/> property.
    ///   Any changes made to the contents of the buffer after calling <see cref="Reset"/> will be reflected in this instance.
    /// </para>
    /// <note>
    ///   Calling the <see cref="Read"/> method may overwrite the contents of the current buffer at offset zero, or allocate a new one if that buffer
    ///   is not big enough.
    /// </note>
    /// </remarks>
    public void Reset(byte[]? buffer, int offset, int count)
    {
        Buffer = buffer;
        Offset = offset;
        Count = count;
    }

    /// <summary>
    /// Writes the object to the specified writer.
    /// </summary>
    /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
    public void Write(BinaryWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (Buffer == null)
        {
            WritableUtility.Write7BitEncodedInt32(writer, 0);
        }
        else
        {
            WritableUtility.Write7BitEncodedInt32(writer, Count);
            writer.Write(Buffer, Offset, Count);
        }
    }

    /// <summary>
    /// Reads the object from the specified reader.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
    public void Read(BinaryReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        Offset = 0;
        Count = WritableUtility.Read7BitEncodedInt32(reader);
        if (Buffer == null || Buffer.Length < Count)
        {
            Buffer = new byte[Count];
        }

        reader.Read(Buffer, 0, Count);
    }
}
