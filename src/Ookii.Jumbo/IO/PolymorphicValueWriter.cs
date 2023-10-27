using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace Ookii.Jumbo.IO;

public sealed class PolymorphicValueWriter<T> : IValueWriter<T>
    where T : notnull
{
    private static readonly Dictionary<string, WriterHelper> _derivedTypes = BuildDerivedTypes();

    public T Read(BinaryReader reader)
    {
        var typeName = reader.ReadString();
        var helper = GetHelper(typeName);
        return (T)helper.Read(reader);
    }

    public void Write(T value, BinaryWriter writer)
    {
        var typeName = value.GetType().FullName ?? value.GetType().Name;
        var helper = GetHelper(typeName);
        writer.Write(typeName);
        helper.Write(value, writer);
    }

    private static WriterHelper GetHelper(string typeName)
    {
        if (!_derivedTypes.TryGetValue(typeName, out var helper))
        {
            throw new InvalidOperationException($"Unsupported type {typeName} in serialized data.");
        }

        return helper;
    }

    private static Dictionary<string, WriterHelper> BuildDerivedTypes()
    {
        var result = new Dictionary<string, WriterHelper>();
        foreach (var attr in typeof(T).GetCustomAttributes<WritableDerivedTypeAttribute>())
        {
            var helper = (WriterHelper)Activator.CreateInstance(typeof(WriterHelper<>).MakeGenericType(attr.DerivedType))!;
            result.Add(attr.DerivedType.FullName!, helper);
        }

        return result;
    }
}

abstract class WriterHelper
{
    public abstract object Read(BinaryReader writer);

    public abstract void Write(object value, BinaryWriter writer);
}

sealed class WriterHelper<TDerived> : WriterHelper
    where TDerived : notnull
{
    public override object Read(BinaryReader reader)
        => ValueWriter<TDerived>.ReadValue(reader);

    public override void Write(object value, BinaryWriter writer)
        => ValueWriter<TDerived>.WriteValue((TDerived)value, writer);
}
