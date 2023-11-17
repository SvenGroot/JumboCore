// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Utility class for creating <see cref="Pair{TKey,TValue}"/> instances.
/// </summary>
public static class Pair
{
    /// <summary>
    /// Creates a pair with the specified key and value.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    /// <param name="key">The key. May be <see langword="null"/> if <typeparamref name="TKey"/> is a reference type.</param>
    /// <param name="value">The value. May be <see langword="null"/> if <typeparamref name="TValue"/> is a reference type.</param>
    /// <returns>An instance of <see cref="Pair{TKey,TValue}"/>.</returns>
    /// <remarks>
    /// <para>
    ///   This function can be used to create a pair using type inference for the key and value types so you do not have to explicitly specify them.
    /// </para>
    /// </remarks>
    public static Pair<TKey, TValue> MakePair<TKey, TValue>(TKey? key, TValue? value)
        where TKey : notnull
        where TValue : notnull
    {
        return new Pair<TKey, TValue>(key, value);
    }
}

/// <summary>
/// Defines a key/value pair that is mutable and supports Jumbo's <see cref="IWritable"/> serialization protocol.
/// </summary>
/// <typeparam name="TKey">The type of the key.</typeparam>
/// <typeparam name="TValue">The type of the value.</typeparam>
/// <remarks>
/// <para>
///   Jumbo does not support the use of <see cref="KeyValuePair{TKey,TValue}"/> as a record type. You must use <see cref="Pair{TKey,TValue}"/> instead.
/// </para>
/// <para>
///   The <see cref="IComparable{T}"/> implementation of this class uses the key only; it ignores the value. <see cref="IEquatable{T}"/> does use
///   both the key and value.
/// </para>
/// </remarks>
[RawComparer(typeof(PairRawComparer<,>))]
public sealed class Pair<TKey, TValue> : IWritable, IComparable<Pair<TKey, TValue>>, IEquatable<Pair<TKey, TValue>>, ICloneable
    where TKey : notnull
    where TValue : notnull
{
    private static readonly IComparer<TKey> _keyComparer = Comparer<TKey>.Default;
    private static readonly IValueWriter<TKey>? _keyWriter = ValueWriter<TKey>.Writer;
    private static readonly IValueWriter<TValue>? _valueWriter = ValueWriter<TValue>.Writer;

    /// <summary>
    /// Initializes a new instance of the <see cref="Pair{TKey,TValue}"/> class.
    /// </summary>
    public Pair()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Pair{TKey,TValue}"/> class with the specified key and value.
    /// </summary>
    /// <param name="key">The key of the key/value pair.</param>
    /// <param name="value">The value of the key/value pair.</param>
    public Pair(TKey? key, TValue? value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>
    /// Gets or sets the key in the key/value pair.
    /// </summary>
    public TKey? Key { get; set; }

    /// <summary>
    /// Gets or sets the value in the key/value pair.
    /// </summary>
    public TValue? Value { get; set; }

    /// <summary>
    /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="Pair{TKey, TValue}"/>.
    /// </summary>
    /// <param name="obj">The <see cref="Object"/> to compare with the current <see cref="Pair{TKey, TValue}"/>.</param>
    /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
    /// <see cref="Pair{TKey, TValue}"/>; otherwise, <see langword="false"/>.</returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as Pair<TKey, TValue>);
    }

    /// <summary>
    /// Serves as a hash function for a particular type. 
    /// </summary>
    /// <returns>A hash code for the current <see cref="Pair{TKey, TValue}"/> based on the key of the underlying <see cref="KeyValuePair{TKey, TValue}"/>.</returns>
    public override int GetHashCode()
    {
        return Key == null ? 0 : Key.GetHashCode();
    }

    /// <summary>
    /// Determines whether two specified <see cref="Pair{TKey, TValue}"/> objects have the same value.
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator ==(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return object.Equals(left, right);
    }

    /// <summary>
    /// Determines whether two specified <see cref="Pair{TKey, TValue}"/> objects have different values.
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return !object.Equals(left, right);
    }

    /// <summary>
    /// Determines whether one specified <see cref="Pair{TKey, TValue}"/> is less than another specified <see cref="Pair{TKey, TValue}"/>
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator <(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return Comparer<Pair<TKey, TValue>>.Default.Compare(left, right) < 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Pair{TKey, TValue}"/> is less than or equal to another specified <see cref="Pair{TKey, TValue}"/>
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator <=(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return Comparer<Pair<TKey, TValue>>.Default.Compare(left, right) <= 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Pair{TKey, TValue}"/> is greater than another specified <see cref="Pair{TKey, TValue}"/>
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator >(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return Comparer<Pair<TKey, TValue>>.Default.Compare(left, right) > 0;
    }

    /// <summary>
    /// Determines whether one specified <see cref="Pair{TKey, TValue}"/> is greater than or equal to another specified <see cref="Pair{TKey, TValue}"/>
    /// </summary>
    /// <param name="left">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <param name="right">A <see cref="Pair{TKey, TValue}"/> or <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator >=(Pair<TKey, TValue>? left, Pair<TKey, TValue>? right)
    {
        return Comparer<Pair<TKey, TValue>>.Default.Compare(left, right) >= 0;
    }

    #region IWritable Members

    /// <summary>
    /// Writes the object to the specified writer.
    /// </summary>
    /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
    public void Write(System.IO.BinaryWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (Key == null || Value == null)
            throw new InvalidOperationException("Key and value may not be null.");
        if (_keyWriter == null)
            ((IWritable)Key).Write(writer);
        else
            _keyWriter.Write(Key, writer);

        if (_valueWriter == null)
            ((IWritable)Value).Write(writer);
        else
            _valueWriter.Write(Value, writer);
    }

    /// <summary>
    /// Reads the object from the specified reader.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
    public void Read(System.IO.BinaryReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        if (_keyWriter == null)
        {
            if (Key == null)
                Key = (TKey)WritableUtility.GetUninitializedWritable(typeof(TKey));
            ((IWritable)Key).Read(reader);
        }
        else
            Key = _keyWriter.Read(reader);

        if (_valueWriter == null)
        {
            if (Value == null)
                Value = (TValue)WritableUtility.GetUninitializedWritable(typeof(TValue));
            ((IWritable)Value).Read(reader);
        }
        else
            Value = _valueWriter.Read(reader);
    }

    /// <summary>
    /// Gets a string representation of the current <see cref="Pair{TKey,TValue}"/>.
    /// </summary>
    /// <returns>A string representation of the current <see cref="Pair{TKey,TValue}"/>.</returns>
    public override string ToString()
    {
        return string.Format(System.Globalization.CultureInfo.CurrentCulture, "[{0}, {1}]", Key, Value);
    }

    #endregion

    #region IComparable<KeyValuePairWritable<TKey,TValue>> Members

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that 
    /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
    /// sort order as the other object. 
    /// </summary>
    /// <param name="other">An object to compare with this instance.</param>
    /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
    public int CompareTo(Pair<TKey, TValue>? other)
    {
        if (other is null)
            return 1;
        return _keyComparer.Compare(Key, other.Key);
    }

    #endregion

    #region IEquatable<KeyValuePairWritable<TKey,TValue>> Members

    /// <summary>
    /// Determines whether the specified <see cref="Pair{TKey,TValue}"/> is equal to the current <see cref="Pair{TKey, TValue}"/>.
    /// </summary>
    /// <param name="other">The <see cref="Object"/> to compare with the current <see cref="Pair{TKey, TValue}"/>.</param>
    /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
    /// <see cref="Pair{TKey, TValue}"/>; otherwise, <see langword="false"/>.</returns>
    public bool Equals(Pair<TKey, TValue>? other)
    {
        if (other is null)
            return false;

        return object.Equals(Key, other.Key) && object.Equals(Value, other.Value);
    }

    #endregion

    #region ICloneable Members

    object ICloneable.Clone()
    {
        var clone = new Pair<TKey, TValue>();
        if (typeof(TKey).IsValueType)
            clone.Key = Key;
        else if (Key != null)
            clone.Key = (TKey)((ICloneable)Key).Clone();
        if (typeof(TValue).IsValueType)
            clone.Value = Value;
        else if (Value != null)
            clone.Value = (TValue)((ICloneable)Value).Clone();
        return clone;
    }

    #endregion
}
