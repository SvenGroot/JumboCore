// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;

namespace Ookii.Jumbo.IO;

/// <summary>
/// A mutable string stored and serialized in utf-8 format.
/// </summary>
/// <remarks>
/// <note type="warning">
///   The <see cref="Utf8String"/> class does not provide full utf-8 string manipulation. Its
///   primary use is as a mutable serialization format, providing a <see cref="IWritable"/>
///   record type that allows record reuse for strings. For string manipulation, conversion
///   to/from <see cref="string"/> will typically be necessary.
/// </note>
/// <note>
///   Instances of the <see cref="Utf8String"/> class are compared per byte, and will not sort
///   in proper lexicographical order if the string contains multi-byte characters. The only
///   guarantee is that the ordering is deterministic.
/// </note>
/// <para>
///   Because this object is mutable you must take care when using it scenarios where immutability is expected, e.g. as a key
///   in a <see cref="Dictionary{TKey,TValue}"/>.
/// </para>
/// </remarks>
[RawComparer(typeof(Utf8StringRawComparer))]
public sealed class Utf8String : IWritable, IEquatable<Utf8String>, IComparable<Utf8String>, IComparable, ICloneable,
    ISpanFormattable, IUtf8SpanFormattable, ISpanParsable<Utf8String>, IUtf8SpanParsable<Utf8String>
{
    private static readonly Encoding _encoding = Encoding.UTF8;
    private byte[] _utf8Bytes;
    private int _byteLength;

    /// <summary>
    /// Initializes a new instance of the <see cref="Utf8String"/> class.
    /// </summary>
    public Utf8String()
    {
        _utf8Bytes = [];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified string.
    /// </summary>
    /// <param name="value">The <see cref="String"/> to set the value to. May be <see langword="null"/>.</param>
    public Utf8String(string? value)
    {
        Set(value);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified
    /// span of characters.
    /// </summary>
    /// <param name="value">A span of characters.</param>
    public Utf8String(ReadOnlySpan<char> value)
    {
        Set(value);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified
    /// utf-8 span.
    /// </summary>
    /// <param name="value">A span containing a utf-8 encoded string.</param>
    /// <remarks>
    /// <note type="warning">
    ///   This constructor does not check if the span contains valid utf-8 data.
    /// </note>
    /// <para>
    ///   The contents of the span will be copied into the new instance.
    /// </para>
    /// </remarks>
    public Utf8String(ReadOnlySpan<byte> value)
    {
        Set(value);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Utf8String"/> class that is a copy of the specified <see cref="Utf8String"/>.
    /// </summary>
    /// <param name="value">The <see cref="Utf8String"/> to copy.</param>
    public Utf8String(Utf8String value)
    {
        Set(value);
    }

    /// <summary>
    /// Gets the bytes of the utf-8 encoded string.
    /// </summary>
    /// <value>A span containing the utf-8 data.</value>
    public ReadOnlySpan<byte> Bytes => _utf8Bytes.AsSpan(0, _byteLength);

    /// <summary>
    /// Gets the number of bytes in the encoded string.
    /// </summary>
    public int ByteLength
    {
        get { return _byteLength; }
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Length less than zero.");
            }

            if (value > _byteLength)
            {
                throw new ArgumentException("Cannot increase string length.");
            }

            _byteLength = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of the string this instance can hold without
    /// resizing.
    /// </summary>
    public int Capacity
    {
        get => _utf8Bytes.Length;
        set
        {
            if (value < _byteLength)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "New capacity is too small");
            }

            var capacity = GetCapacityNeeded(value);
            Array.Resize(ref _utf8Bytes, capacity);
        }
    }

    /// <summary>
    /// Gets the length of the string in characters.
    /// </summary>
    public int CharLength
    {
        get
        {
            return _encoding.GetCharCount(Bytes);
        }
    }

    /// <summary>
    /// Sets the value of this <see cref="Utf8String"/> to the specified span of characters.
    /// </summary>
    /// <param name="value">A span of characters.</param>
    /// <remarks>
    /// <para>
    ///   The contents of the span will be copied into this instance.
    /// </para>
    /// </remarks>
    [MemberNotNull(nameof(_utf8Bytes))]
    public void Set(ReadOnlySpan<char> value)
    {
        _byteLength = _encoding.GetByteCount(value);
        if (_utf8Bytes == null || Capacity < _byteLength)
        {
            _utf8Bytes = new byte[GetCapacityNeeded(_byteLength)];
        }

        _encoding.GetBytes(value, _utf8Bytes);
    }

    /// <summary>
    /// Sets the value of this <see cref="Utf8String"/> to the specified <see cref="string"/>.
    /// </summary>
    /// <param name="value">The <see cref="string"/> to set the value to. May be <see langword="null"/>.</param>
    /// <remarks>
    /// <para>
    ///   The contents of the string will be copied into this instance.
    /// </para>
    /// </remarks>
    [MemberNotNull(nameof(_utf8Bytes))]
    public void Set(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            _byteLength = 0;
            _utf8Bytes = [];
        }
        else
        {
            Set(value.AsSpan());
        }
    }

    /// <summary>
    /// Sets the value of this <see cref="Utf8String"/> to the specified region of the specified byte array.
    /// </summary>
    /// <param name="value">A span of bytes containing a utf-8 encoded string.</param>
    /// <remarks>
    /// <note type="warning">
    ///   This method does not check if the span contains valid utf-8 data.
    /// </note>
    /// <para>
    ///   The contents of the span will be copied into this instance.
    /// </para>
    /// </remarks>
    [MemberNotNull(nameof(_utf8Bytes))]
    public void Set(ReadOnlySpan<byte> value)
    {
        var capacityNeeded = GetCapacityNeeded(value.Length);
        if (_utf8Bytes == null || _utf8Bytes.Length < capacityNeeded)
        {
            _utf8Bytes = new byte[capacityNeeded];
        }

        value.CopyTo(_utf8Bytes);
        _byteLength = value.Length;
    }

    /// <summary>
    /// Sets the value of this <see cref="Utf8String"/> to the value of the specified <see cref="Utf8String"/>.
    /// </summary>
    /// <param name="value">The <see cref="Utf8String"/> to copy.</param>
    [MemberNotNull(nameof(_utf8Bytes))]
    public void Set(Utf8String value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Set(value.Bytes);
    }

    /// <summary>
    /// Appends the specified <see cref="Utf8String"/> to this instance..
    /// </summary>
    /// <param name="value">The <see cref="Utf8String"/> to append.</param>
    public void Append(Utf8String value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Append(value.Bytes);
    }

    /// <summary>
    /// Appends a span containing utf-8 encoded data to this string.
    /// </summary>
    /// <param name="value">A span containing the utf-8 encoded string to append.</param>
    /// <remarks>
    /// <note type="warning">
    ///   This method does not check if the span contains valid utf-8 data.
    /// </note>
    /// </remarks>
    public void Append(ReadOnlySpan<byte> value)
    {
        if (_byteLength == 0)
        {
            Set(value);
        }
        else
        {
            var newCapacity = Capacity;
            var newSize = _byteLength + value.Length;
            while (newSize > newCapacity)
            {
                newCapacity <<= 2;
            }

            if (newCapacity != Capacity)
            {
                Capacity = newCapacity;
            }

            value.CopyTo(_utf8Bytes.AsSpan(_byteLength));
            _byteLength = newSize;
        }
    }

    /// <summary>
    /// Gets a string representation of the current <see cref="Utf8String"/>.
    /// </summary>
    /// <returns>A string representation of the current <see cref="Utf8String"/>.</returns>
    public override string ToString() => _encoding.GetString(Bytes);

    /// <summary>
    /// Writes this <see cref="Utf8String"/> to the specified stream.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <remarks>
    ///   This writes the utf-8 byte data of the string to the stream. No other information (such as the string length) is written.
    /// </remarks>
    public void Write(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);

        stream.Write(Bytes);
    }

    /// <summary>
    /// Gets a hash code for this <see cref="Utf8String"/>.
    /// </summary>
    /// <returns>A 32-bit hash code for this <see cref="Utf8String"/>.</returns>
    public override int GetHashCode()
    {
        var hash = 1;
        for (var i = 0; i < _byteLength; i++)
        {
            hash = (31 * hash) + _utf8Bytes[i];
        }

        return hash;
    }

    /// <summary>
    /// Tests this <see cref="Utf8String"/> for equality with the specified object.
    /// </summary>
    /// <param name="obj">The <see cref="Object"/> to test for equality.</param>
    /// <returns><see langword="true"/> if this instance is equal to <paramref name="obj"/>; otherwise, <see langword="false"/>.</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as Utf8String);
    }

    /// <summary>
    /// Gets the length of a Utf8String stored in a byte array.
    /// </summary>
    /// <param name="buffer">The buffer containing the string.</param>
    /// <param name="index">The index at which the string starts.</param>
    /// <returns>The length of the entire Utf8String object, including the length header.</returns>
    public static int GetLength(byte[] buffer, int index)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (index < 0 || index >= buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        var offset = index;
        var length = LittleEndianBitConverter.ToInt32From7BitEncoding(buffer, ref offset);
        return length + (offset - index);
    }

    /// <inheritdoc/>
    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
        => _encoding.TryGetChars(Bytes, destination, out charsWritten);

    /// <inheritdoc/>
    public string ToString(string? format, IFormatProvider? formatProvider) => ToString();

    /// <inheritdoc/>
    public bool TryFormat(Span<byte> utf8Destination, out int bytesWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
    {
        var source = Bytes;
        var copySize = Math.Min(utf8Destination.Length, source.Length);
        source[..copySize].CopyTo(utf8Destination);
        bytesWritten = copySize;
        return true;
    }

    /// <inheritdoc/>
    public static Utf8String Parse(ReadOnlySpan<char> s, IFormatProvider? provider) => new(s);

    /// <inheritdoc/>
    public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, [MaybeNullWhen(false)] out Utf8String result)
    {
        result = new(s);
        return true;
    }

    /// <inheritdoc/>
    public static Utf8String Parse(string s, IFormatProvider? provider) => new(s);

    /// <inheritdoc/>
    public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, [MaybeNullWhen(false)] out Utf8String result)
    {
        result = new(s);
        return true;
    }

    /// <inheritdoc/>
    public static Utf8String Parse(ReadOnlySpan<byte> utf8Text, IFormatProvider? provider) => new(utf8Text);

    /// <inheritdoc/>
    public static bool TryParse(ReadOnlySpan<byte> utf8Text, IFormatProvider? provider, [MaybeNullWhen(false)] out Utf8String result)
    {
        result = new(utf8Text);
        return true;
    }

    /// <summary>
    /// Determines whether two specified <see cref="Utf8String"/> objects have the same value.
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator ==(Utf8String? left, Utf8String? right)
    {
        return object.Equals(left, right);
    }

    /// <summary>
    /// Determines whether two specified <see cref="Utf8String"/> objects have different values.
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(Utf8String? left, Utf8String? right)
    {
        return !object.Equals(left, right);
    }

    /// <summary>
    /// Determines whether one specified <see cref="Utf8String"/> is less than another specified <see cref="Utf8String"/>
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator <(Utf8String? left, Utf8String? right)
    {
        return Comparer<Utf8String>.Default.Compare(left, right) < 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Utf8String"/> is less than or equal to another specified <see cref="Utf8String"/>
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator <=(Utf8String? left, Utf8String? right)
    {
        return Comparer<Utf8String>.Default.Compare(left, right) <= 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Utf8String"/> is greater than another specified <see cref="Utf8String"/>
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator >(Utf8String? left, Utf8String? right)
    {
        return Comparer<Utf8String>.Default.Compare(left, right) > 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Utf8String"/> is greater than or equal to another specified <see cref="Utf8String"/>
    /// </summary>
    /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator >=(Utf8String? left, Utf8String? right)
    {
        return Comparer<Utf8String>.Default.Compare(left, right) >= 0;
    }

    #region IEquatable<Utf8StringWritable> Members

    /// <summary>
    /// Tests this <see cref="Utf8String"/> for equality with the specified <see cref="Utf8String"/>.
    /// </summary>
    /// <param name="other">The <see cref="Utf8String"/> to test for equality.</param>
    /// <returns><see langword="true"/> if this instance is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
    public bool Equals(Utf8String? other)
    {
        if (ReferenceEquals(other, this))
        {
            return true;
        }

        if (other is null || other._byteLength != _byteLength)
        {
            return false;
        }

        return UnsafeCompare(_utf8Bytes, _byteLength, other._utf8Bytes, _byteLength) == 0;
    }

    #endregion

    #region IComparable<Utf8StringWritable> Members

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that 
    /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
    /// sort order as the other object. 
    /// </summary>
    /// <param name="other">An object to compare with this instance.</param>
    /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
    public int CompareTo(Utf8String? other)
    {
        if (other is null)
        {
            return 1;
        }

        if (ReferenceEquals(other, this))
        {
            return 0;
        }

        return UnsafeCompare(_utf8Bytes, _byteLength, other._utf8Bytes, other._byteLength);
    }

    #endregion

    #region IComparable Members

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that 
    /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
    /// sort order as the other object. 
    /// </summary>
    /// <param name="obj">An object to compare with this instance.</param>
    /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
    public int CompareTo(object? obj)
    {
        return CompareTo(obj as Utf8String);
    }

    #endregion

    #region IWritable Members

    /// <summary>
    /// Reads the object from the specified reader.
    /// </summary>
    /// <param name="reader">The <see cref="System.IO.BinaryReader"/> to deserialize the object from.</param>
    public void Read(System.IO.BinaryReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var length = WritableUtility.Read7BitEncodedInt32(reader);
        if (_utf8Bytes != null && length <= Capacity)
        {
            var totalRead = 0;
            do
            {
                var bytesRead = reader.Read(_utf8Bytes, totalRead, length - totalRead);
                if (bytesRead == 0)
                {
                    throw new FormatException("Invalid Utf8StringWritable detected in stream.");
                }

                totalRead += bytesRead;
            } while (totalRead < length);
            _byteLength = length;
        }
        else
        {
            Set(reader.ReadBytes(length));
        }
    }

    /// <summary>
    /// Writes the object to the specified writer.
    /// </summary>
    /// <param name="writer">The <see cref="System.IO.BinaryWriter"/> to serialize the object to.</param>
    public void Write(System.IO.BinaryWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        WritableUtility.Write7BitEncodedInt32(writer, _byteLength);
        writer.Write(Bytes);
    }

    #endregion

    #region ICloneable Members

    /// <summary>
    /// Creates a clone of the current <see cref="Utf8String"/>.
    /// </summary>
    /// <returns>A new <see cref="Utf8String"/> with the same value as the current instance.</returns>
    public object Clone()
    {
        return new Utf8String(this);
    }

    #endregion

    private static int GetCapacityNeeded(int size)
    {
        // Round to multiple of 4
        unchecked
        {
            return (size & (int)0xFFFFFFFC) + 4;
        }
    }

    private static unsafe int UnsafeCompare(byte[] str1, int length1, byte[] str2, int length2)
    {
        fixed (byte* str1ptr = str1, str2ptr = str2)
        {
            var left = str1ptr;
            var end = left + Math.Min(length1, length2);
            var right = str2ptr;
            while (left < end)
            {
                if (*left != *right)
                {
                    return *left - *right;
                }

                ++left;
                ++right;
            }
            return length1 - length2;
        }
    }
}
