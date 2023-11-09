using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Value writer that allows a base class to be polymorphically serialized.
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
/// <remarks>
/// <para>
///   When using this value writer, you must declare all allowed derived using the <see cref="WritableDerivedTypeAttribute"/>
///   attribute.
/// </para>
/// <para>
///   In order to support serializing the base class itself, it must not be <see langword="abstract"/>
///   and implement the <see cref="IWritable"/> interface.
/// </para>
/// </remarks>
public sealed class PolymorphicValueWriter<T> : IValueWriter<T>
    where T : notnull
{
    private interface IWriterHelper
    {
        T Read(BinaryReader writer);

        void Write(T value, BinaryWriter writer);
    }

    private sealed class WriterHelper<TDerived> : IWriterHelper
        where TDerived : notnull, T
    {
        public T Read(BinaryReader reader)
            => ValueWriter<TDerived>.ReadValue(reader);

        public void Write(T value, BinaryWriter writer)
            => ValueWriter<TDerived>.WriteValue((TDerived)value, writer);
    }

    private static readonly ImmutableDictionary<string, IWriterHelper> _derivedTypes = BuildDerivedTypes();
    private static readonly bool _allowBaseClass = !typeof(T).IsAbstract && typeof(T).IsAssignableTo(typeof(IWritable));

    /// <inheritdoc/>
    public T Read(BinaryReader reader)
    {
        var typeName = reader.ReadString();
        if (_allowBaseClass && typeName.Length == 0)
        {
            var result = WritableUtility.GetUninitializedWritable(typeof(T));
            result.Read(reader);
            return (T)result;
        }
        else
        {
            var helper = GetHelper(typeName);
            return helper.Read(reader);
        }
    }

    /// <inheritdoc/>
    public void Write(T value, BinaryWriter writer)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(writer);
        if (_allowBaseClass && value.GetType() == typeof(T))
        {
            writer.Write(string.Empty);
            ((IWritable)value).Write(writer);
        }
        else
        {
            var typeName = value.GetType().FullName ?? value.GetType().Name;
            var helper = GetHelper(typeName);
            writer.Write(typeName);
            helper.Write(value, writer);
        }
    }

    private static IWriterHelper GetHelper(string typeName)
    {
        if (!_derivedTypes.TryGetValue(typeName, out var helper))
        {
            throw new InvalidOperationException($"Unsupported type {typeName} in serialized data.");
        }

        return helper;
    }

    private static ImmutableDictionary<string, IWriterHelper> BuildDerivedTypes()
    {
        return typeof(T).GetCustomAttributes<WritableDerivedTypeAttribute>()
            .Select(attr => KeyValuePair.Create(attr.DerivedType.FullName!,
                (IWriterHelper)Activator.CreateInstance(typeof(WriterHelper<>).MakeGenericType(typeof(T), attr.DerivedType))!))
            .ToImmutableDictionary();
    }
}
