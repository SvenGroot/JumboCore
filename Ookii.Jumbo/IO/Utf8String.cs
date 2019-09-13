// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A mutable string stored and serialized in utf-8 format.
    /// </summary>
    /// <remarks>
    /// <note>
    ///   Instances of the <see cref="Utf8String"/> class will not compare in proper lexicographical order if the string contains multi-byte characters.
    ///   All that is guaranteed is that the ordering is deterministic.
    /// </note>
    /// <para>
    ///   Because this object is mutable you must take care when using it scenarios where immutability is expected, e.g. as a key
    ///   in a <see cref="Dictionary{TKey,TValue}"/>.
    /// </para>
    /// </remarks>
    [RawComparer(typeof(Utf8StringRawComparer))]
    public sealed class Utf8String : IWritable, IEquatable<Utf8String>, IComparable<Utf8String>, IComparable, ICloneable
    {
        private static readonly Encoding _encoding = Encoding.UTF8;
        private static readonly byte[] _emptyArray = new byte[0];
        private byte[] _utf8Bytes;
        private int _byteLength;

        /// <summary>
        /// Initializes a new instance of the <see cref="Utf8String"/> class.
        /// </summary>
        public Utf8String()
        {
            _utf8Bytes = _emptyArray;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified string.
        /// </summary>
        /// <param name="value">The <see cref="String"/> to set the value to. May be <see langword="null"/>.</param>
        public Utf8String(string value)
        {
            Set(value);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified utf-8 byte array.
        /// </summary>
        /// <param name="value">A byte array containing a utf-8 encoded string.</param>
        public Utf8String(byte[] value)
        {
            Set(value);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Utf8String"/> class using the specified utf-8 byte array, index and count.
        /// </summary>
        /// <param name="value">A byte array containing a utf-8 encoded string.</param>
        /// <param name="index">The index in <paramref name="value"/> to start copying.</param>
        /// <param name="count">The number of bytes from <paramref name="value"/> to copy.</param>
        public Utf8String(byte[] value, int index, int count)
        {
            Set(value, index, count);
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
        /// Gets the number of bytes in the encoded string.
        /// </summary>
        public int ByteLength
        {
            get { return _byteLength; }
            set
            {
                if( value < 0 )
                    throw new ArgumentOutOfRangeException("value", "Length less than zero.");
                if( value > _byteLength )
                    throw new ArgumentException("Cannot increase string length.");
                _byteLength = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum size, in bytes, of the string this instance can hold without resizing.
        /// </summary>
        public int Capacity
        {
            get { return _utf8Bytes == null ? 0 : _utf8Bytes.Length; }
            set
            {
                if( value < _byteLength )
                    throw new ArgumentOutOfRangeException("value", "New capacity is too small");
                int capacity = GetCapacityNeeded(value);
                byte[] newArray = new byte[capacity];
                Array.Copy(_utf8Bytes, newArray, _byteLength);
                _utf8Bytes = newArray;
            }
        }

        /// <summary>
        /// Gets the length of the string in characters.
        /// </summary>
        public int CharLength
        {
            get
            {
                return _encoding.GetCharCount(_utf8Bytes, 0, _byteLength);
            }
        }

        /// <summary>
        /// Sets the value of this <see cref="Utf8String"/> to the specified <see cref="String"/>.
        /// </summary>
        /// <param name="value">The <see cref="String"/> to set the value to. May be <see langword="null"/>.</param>
        public void Set(string value)
        {
            if( string.IsNullOrEmpty(value) )
            {
                _byteLength = 0;
            }
            else
            {
                _byteLength = _encoding.GetByteCount(value);
                if( Capacity < _byteLength )
                    _utf8Bytes = new byte[GetCapacityNeeded(_byteLength)];
                _encoding.GetBytes(value, 0, value.Length, _utf8Bytes, 0);
            }
        }

        /// <summary>
        /// Sets the value of this <see cref="Utf8String"/> to the specified byte array.
        /// </summary>
        /// <param name="value">A byte array containing a utf-8 encoded string.</param>
        public void Set(byte[] value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            Set(value, 0, value.Length);
        }

        /// <summary>
        /// Sets the value of this <see cref="Utf8String"/> to the specified region of the specified byte array.
        /// </summary>
        /// <param name="value">A byte array containing a utf-8 encoded string.</param>
        /// <param name="index">The index in <paramref name="value"/> to start copying.</param>
        /// <param name="count">The number of bytes from <paramref name="value"/> to copy.</param>
        public void Set(byte[] value, int index, int count)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            int capacityNeeded = GetCapacityNeeded(count);
            if( _utf8Bytes == null || _utf8Bytes.Length < capacityNeeded )
                _utf8Bytes = new byte[capacityNeeded];
            Array.Copy(value, index, _utf8Bytes, 0, count);
            _byteLength = count;
        }

        /// <summary>
        /// Sets the value of this <see cref="Utf8String"/> to the value of the specified <see cref="Utf8String"/>.
        /// </summary>
        /// <param name="value">The <see cref="Utf8String"/> to copy.</param>
        public void Set(Utf8String value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            Set(value._utf8Bytes, 0, value._byteLength);
        }

        /// <summary>
        /// Appends the specified <see cref="Utf8String"/> to this instance..
        /// </summary>
        /// <param name="value">The <see cref="Utf8String"/> to append.</param>
        public void Append(Utf8String value)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            Append(value._utf8Bytes, 0, value.ByteLength);
        }

        /// <summary>
        /// Appends a byte array containing utf-8 encoded data to this string.
        /// </summary>
        /// <param name="value">A byte array containing the utf-8 encoded string to append.</param>
        /// <param name="index">The index in <paramref name="value"/> at which to start copying.</param>
        /// <param name="count">The number of bytes from <paramref name="value"/> to copy.</param>
        public void Append(byte[] value, int index, int count)
        {
            if( value == null )
                throw new ArgumentNullException("value");

            if( Capacity == 0 )
            {
                Set(value, index, count);
            }
            else
            {
                int newCapacity = Capacity;
                int newSize = _byteLength + count;
                while( newSize > newCapacity )
                {
                    newCapacity <<= 2;
                }
                if( newCapacity != Capacity )
                    Capacity = newCapacity;

                Array.Copy(value, index, _utf8Bytes, _byteLength, count);
                _byteLength = newSize;
            }
        }

        /// <summary>
        /// Gets a string representation of the current <see cref="Utf8String"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="Utf8String"/>.</returns>
        public override string ToString()
        {
            return _encoding.GetString(_utf8Bytes, 0, _byteLength);
        }

        /// <summary>
        /// Writes this <see cref="Utf8String"/> to the specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <remarks>
        ///   This writes the utf-8 byte data of the string to the stream. No other information (such as the string length) is written.
        /// </remarks>
        public void Write(Stream stream)
        {
            if( stream == null )
                throw new ArgumentNullException("stream");

            stream.Write(_utf8Bytes, 0, _byteLength);
        }

        /// <summary>
        /// Gets the bytes of the utf-8 encoded string.
        /// </summary>
        /// <returns>The utf-8 encoded string.</returns>
        public byte[] GetBytes()
        {
            byte[] result = new byte[_byteLength];
            Buffer.BlockCopy(_utf8Bytes, 0, result, 0, _byteLength);
            return result;
        }

        /// <summary>
        /// Gets a hash code for this <see cref="Utf8String"/>.
        /// </summary>
        /// <returns>A 32-bit hash code for this <see cref="Utf8String"/>.</returns>
        public override int GetHashCode()
        {
            int hash = 1;
            for( int i = 0; i < _byteLength; i++ )
                hash = (31 * hash) + (int)_utf8Bytes[i];
            return hash;
        }

        /// <summary>
        /// Tests this <see cref="Utf8String"/> for equality with the specified object.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to test for equality.</param>
        /// <returns><see langword="true"/> if this instance is equal to <paramref name="obj"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
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
            if( buffer == null )
                throw new ArgumentNullException("buffer");
            if( index < 0 || index >= buffer.Length )
                throw new ArgumentOutOfRangeException("index");

            int offset = index;
            int length = LittleEndianBitConverter.ToInt32From7BitEncoding(buffer, ref offset);
            return length + (offset - index);
        }

        /// <summary>
        /// Determines whether two specified <see cref="Utf8String"/> objects have the same value.
        /// </summary>
        /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(Utf8String left, Utf8String right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Determines whether two specified <see cref="Utf8String"/> objects have different values.
        /// </summary>
        /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(Utf8String left, Utf8String right)
        {
            return !object.Equals(left, right);
        }

        /// <summary>
        /// Determines whether one specified <see cref="Utf8String"/> is less than another specified <see cref="Utf8String"/>
        /// </summary>
        /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(Utf8String left, Utf8String right)
        {
            return Comparer<Utf8String>.Default.Compare(left, right) < 0;
        }

        /// <summary>
        /// Determines whether one specified <see cref="Utf8String"/> is greater than another specified <see cref="Utf8String"/>
        /// </summary>
        /// <param name="left">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="Utf8String"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(Utf8String left, Utf8String right)
        {
            return Comparer<Utf8String>.Default.Compare(left, right) > 0;
        }

        #region IEquatable<Utf8StringWritable> Members

        /// <summary>
        /// Tests this <see cref="Utf8String"/> for equality with the specified <see cref="Utf8String"/>.
        /// </summary>
        /// <param name="other">The <see cref="Utf8String"/> to test for equality.</param>
        /// <returns><see langword="true"/> if this instance is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
        public bool Equals(Utf8String other)
        {
            if( (object)other == (object)this )
                return true;
            if( (object)other == null || other._byteLength != _byteLength )
                return false;

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
        public int CompareTo(Utf8String other)
        {
            if( (object)other == null )
                return 1;
            if( (object)other == (object)this )
                return 0;

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
        public int CompareTo(object obj)
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
            if( reader == null )
                throw new ArgumentNullException("reader");
            int length = WritableUtility.Read7BitEncodedInt32(reader);
			if( length <= Capacity )
			{
                int totalRead = 0;
                do
                {
                    int bytesRead = reader.Read(_utf8Bytes, totalRead, length - totalRead);
                    if( bytesRead == 0 )
                        throw new FormatException("Invalid Utf8StringWritable detected in stream.");
                    totalRead += bytesRead;
                } while( totalRead < length );
    			_byteLength = length;
			}
			else
			  Set(reader.ReadBytes(length));
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="System.IO.BinaryWriter"/> to serialize the object to.</param>
        public void Write(System.IO.BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException("writer");
            WritableUtility.Write7BitEncodedInt32(writer, _byteLength);
            writer.Write(_utf8Bytes, 0, _byteLength);
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
            fixed( byte* str1ptr = str1, str2ptr = str2 )
            {
                byte* left = str1ptr;
                byte* end = left + Math.Min(length1, length2);
                byte* right = str2ptr;
                while( left < end )
                {
                    if( *left != *right )
                        return *left - *right;
                    ++left;
                    ++right;
                }
                return length1 - length2;
            }
        }
    }
}
