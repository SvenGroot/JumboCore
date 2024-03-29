﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Provides methods for determining the <see cref="IValueWriter{T}"/> for a type.
/// </summary>
/// <remarks>
/// Normally, you should use the <see cref="ValueWriter{T}"/> class to access value writers.
/// </remarks>
public static class ValueWriter
{
    /// <summary>
    /// Gets the <see cref="IValueWriter{T}"/> implementation for the specified type.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <returns>The <see cref="IValueWriter{T}"/> implementation for the specified type, or <see langword="null"/> if the type implements <see cref="IWritable"/>.</returns>
    /// <exception cref="NotSupportedException">The type has no value writer and does not implement <see cref="IWritable"/>.</exception>
    public static object? GetWriter(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        // Check for value writers first to allow PolymorphicValueWriter to work with a base
        // type that implements IWritable.
        var attribute = type.GetCustomAttribute<ValueWriterAttribute>();
        if (attribute != null && !string.IsNullOrEmpty(attribute.ValueWriterTypeName))
        {
            var writerType = Type.GetType(attribute.ValueWriterTypeName, true)!;
            return Activator.CreateInstance(writerType);
        }

        if (!type.IsAbstract && type.GetInterfaces().Contains(typeof(IWritable)))
        {
            return null;
        }

        return DefaultValueWriter.GetWriter(type);
    }

    /// <summary>
    /// Writes the specified value using its <see cref="IWritable"/> implementation or its
    /// <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the object whose <see cref="IWritable"/> or <see cref="IValueWriter{T}"/>
    /// implementation to use.
    /// </typeparam>
    /// <param name="value">The value to write.</param>
    /// <param name="writer">The writer to write the value to.</param>
    /// <remarks>
    /// <para>
    ///   If the type of <paramref name="value"/> implements <see cref="IWritable"/>, it is used
    ///   to write the value. If it does not, the <see cref="IValueWriter{T}"/> is used to write
    ///   the value.
    /// </para>
    /// </remarks>
    public static void WriteValue<T>(T value, BinaryWriter writer)
        where T : notnull
    {
        ValueWriter<T>.WriteValue(value, writer);
    }

    /// <summary>
    /// Writes the specified nullable value using its <see cref="IWritable"/> implementation or
    /// its <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the object to write.
    /// </typeparam>
    /// <param name="value">The nullable value to write.</param>
    /// <param name="writer">The writer to write the value to.</param>
    /// <remarks>
    /// <para>
    ///   A <see cref="bool"/> will be written before the object to indicate whether the value
    ///   is <see langword="null"/>. If it is <see langword="null"/>, nothing else will be
    ///   written.
    /// </para>
    /// <para>
    ///   If the type of <paramref name="value"/> implements <see cref="IWritable"/>, it is used
    ///   to write the value. If it does not, the <see cref="IValueWriter{T}"/> is used to write
    ///   the value.
    /// </para>
    /// </remarks>
    public static void WriteNullable<T>(T? value, BinaryWriter writer)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (value == null)
        {
            writer.Write(false);
        }
        else
        {
            writer.Write(true);
            ValueWriter<T>.WriteValue(value, writer);
        }
    }

    /// <summary>
    /// Writes the specified nullable structure using its <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the object to write.
    /// </typeparam>
    /// <param name="value">The nullable value to write.</param>
    /// <param name="writer">The writer to write the value to.</param>
    /// <remarks>
    /// <para>
    ///   A <see cref="bool"/> will be written before the object to indicate whether the value
    ///   is <see langword="null"/>. If it is <see langword="null"/>, nothing else will be
    ///   written.
    /// </para>
    /// </remarks>
    public static void WriteNullable<T>(T? value, BinaryWriter writer)
        where T : struct
    {
        ArgumentNullException.ThrowIfNull(writer);
        if (value is T actual)
        {
            writer.Write(true);
            ValueWriter<T>.WriteValue(actual, writer);
        }
        else
        {
            writer.Write(false);
        }
    }

    /// <summary>
    /// Reads a nullable value from the specified reader using the type's
    /// <see cref="IWritable"/> implementation or its <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the object to read.
    /// </typeparam>
    /// <param name="reader">The reader to read the value from.</param>
    /// <returns>An object containing the value.</returns>
    /// <remarks>
    /// <para>
    ///   A <see cref="bool"/> is read from <paramref name="reader"/> to see if the value is
    ///   <see langword="null"/>. If it is <see langword="null"/>, nothing else will be
    ///   read.
    /// </para>
    /// <para>
    ///   If the type implements <see cref="IWritable"/>, a new instance is created and <see cref="IWritable.Read"/>
    ///   is used to read the value. If it does not, <see cref="IValueWriter{T}"/> is used to read the value.
    /// </para>
    /// <para>
    ///   This method will always create a new instance, even if the type implements <see cref="IWritable"/>, so
    ///   should not be used in scenarios where you wish to support record reuse.
    /// </para>
    /// </remarks>
    public static T? ReadNullable<T>(BinaryReader reader)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(reader);
        return reader.ReadBoolean() ? ValueWriter<T>.ReadValue(reader) : null;
    }

    /// <summary>
    /// Reads a nullable structure value from the specified reader using the type's
    /// <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the object to read.
    /// </typeparam>
    /// <param name="reader">The reader to read the value from.</param>
    /// <returns>An object containing the value.</returns>
    /// <remarks>
    /// <para>
    ///   A <see cref="bool"/> is read from <paramref name="reader"/> to see if the value is
    ///   <see langword="null"/>. If it is <see langword="null"/>, nothing else will be
    ///   read.
    /// </para>
    /// </remarks>
    public static T? ReadNullableStruct<T>(BinaryReader reader)
        where T : struct
    {
        ArgumentNullException.ThrowIfNull(reader);
        return reader.ReadBoolean() ? ValueWriter<T>.ReadValue(reader) : null;
    }
}

/// <summary>
/// Provides access to <see cref="IValueWriter{T}"/> implementations for various basic framework types and types that specify the <see cref="ValueWriterAttribute"/> attribute.
/// </summary>
/// <typeparam name="T">The type of the object whose <see cref="IWritable"/> or <see cref="IValueWriter{T}"/> implementation to use.</typeparam>
/// <remarks>
/// <para>
///   If you attempt to access this class for a type <typeparamref name="T" /> that does not implement <see cref="IWritable"/> and that does not have a implementation of <see cref="IValueWriter{T}"/>,
///   an <see cref="NotSupportedException"/> is thrown by the static type initializer of the <see cref="ValueWriter{T}"/> class.
/// </para>
/// <para>
///   Built-in value writers are provided for the following types: <see cref="SByte"/>,
///   <see cref="Int16"/>, <see cref="Int32"/>, <see cref="Int64"/>, <see cref="Byte"/>,
///   <see cref="UInt16"/>, <see cref="UInt32"/>, <see cref="UInt64"/>, <see cref="Decimal"/>,
///   <see cref="Single"/>, <see cref="Double"/>, <see cref="String"/>, <see cref="DateTime"/>,
///   <see cref="Boolean"/>, <see cref="ValueTuple{T1}"/>, <see cref="ValueTuple{T1, T2}"/>,
///   <see cref="ValueTuple{T1, T2, T3}"/>, <see cref="ValueTuple{T1, T2, T3, T4}"/>,
///   <see cref="ValueTuple{T1, T2, T3, T4, T5}"/>, <see cref="ValueTuple{T1, T2, T3, T4, T5, T6}"/>,
///   <see cref="ValueTuple{T1, T2, T3, T4, T5, T6, T7}"/>
///   <see cref="ValueTuple{T1, T2, T3, T4, T5, T6, T7, TRest}"/>, <see cref="Tuple{T1}"/>,
///   <see cref="Tuple{T1, T2}"/>, <see cref="Tuple{T1, T2, T3}"/>,
///   <see cref="Tuple{T1, T2, T3, T4}"/>, <see cref="Tuple{T1, T2, T3, T4, T5}"/>,
///   <see cref="Tuple{T1, T2, T3, T4, T5, T6}"/>, <see cref="Tuple{T1, T2, T3, T4, T5, T6, T7}"/>
///   and <see cref="Tuple{T1, T2, T3, T4, T5, T6, T7, TRest}"/>.
/// </para>
/// <para>
///   Although value writers support tuples, there may be negative performance implications from
///   using the reference type tuples, as they are read-only and do not support record reuse.
///   Using value tuples is recommended.
/// </para>
/// <para>
///   The <see cref="KeyValuePair{TKey, TValue}"/> structure is not supported by the value writer. Please use the <see cref="Pair{TKey, TValue}"/> class instead, which provides
///   additional functionality for key/value pairs required by Jumbo.
/// </para>
/// </remarks>
public static class ValueWriter<T>
    where T : notnull
{
    private static readonly IValueWriter<T>? _writer = (IValueWriter<T>?)ValueWriter.GetWriter(typeof(T));

    /// <summary>
    /// Gets the writer for the type, or <see langword="null"/> if it implements <see cref="IWritable"/>.
    /// </summary>
    /// <value>
    /// An implementation of <see cref="IValueWriter{T}"/>, or <see langword="null"/> if it implements <see cref="IWritable"/>.
    /// </value>
    public static IValueWriter<T>? Writer
    {
        get { return _writer; }
    }

    /// <summary>
    /// Writes the specified value using its <see cref="IWritable"/> implementation or its <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <param name="value">The value to write.</param>
    /// <param name="writer">The writer to write the value to.</param>
    /// <remarks>
    /// <para>
    ///   If the type of <paramref name="value"/> implements <see cref="IWritable"/>, it is used to write
    ///   the value. If it does not, the <see cref="IValueWriter{T}"/> is used to write the value.
    /// </para>
    /// </remarks>
    public static void WriteValue(T value, BinaryWriter writer)
    {
        if (_writer == null)
        {
            ((IWritable)value).Write(writer);
        }
        else
        {
            _writer.Write(value, writer);
        }
    }

    /// <summary>
    /// Reads a value from the specified reader using the type's <see cref="IWritable"/> implementation or its <see cref="IValueWriter{T}"/>.
    /// </summary>
    /// <param name="reader">The reader to read the value from.</param>
    /// <returns>An object containing the value.</returns>
    /// <remarks>
    /// <para>
    ///   If the type implements <see cref="IWritable"/>, a new instance is created and <see cref="IWritable.Read"/>
    ///   is used to read the value. If it does not, <see cref="IValueWriter{T}"/> is used to read the value.
    /// </para>
    /// <para>
    ///   This method will always create a new instance, even if the type implements <see cref="IWritable"/>, so
    ///   should not be used in scenarios where you wish to support record reuse.
    /// </para>
    /// </remarks>
    public static T ReadValue(BinaryReader reader)
    {
        if (_writer == null)
        {
            var result = (T)WritableUtility.GetUninitializedWritable(typeof(T));
            ((IWritable)result).Read(reader);
            return result;
        }
        else
        {
            return _writer.Read(reader);
        }
    }
}
