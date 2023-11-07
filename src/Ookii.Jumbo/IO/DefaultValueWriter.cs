// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;

namespace Ookii.Jumbo.IO;

static class DefaultValueWriter
{
    #region Nested types

    private class SByteWriter : IValueWriter<SByte>
    {
        public void Write(SByte value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public SByte Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadSByte();
        }
    }

    private class Int16Writer : IValueWriter<Int16>
    {
        public void Write(Int16 value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Int16 Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadInt16();
        }
    }

    private class Int32Writer : IValueWriter<Int32>
    {
        public void Write(int value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public int Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadInt32();
        }
    }

    private class Int64Writer : IValueWriter<Int64>
    {
        public void Write(Int64 value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Int64 Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadInt64();
        }
    }

    private class ByteWriter : IValueWriter<Byte>
    {
        public void Write(Byte value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Byte Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadByte();
        }
    }

    private class UInt16Writer : IValueWriter<UInt16>
    {
        public void Write(UInt16 value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public UInt16 Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadUInt16();
        }
    }

    private class UInt32Writer : IValueWriter<UInt32>
    {
        public void Write(UInt32 value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public UInt32 Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadUInt32();
        }
    }

    private class UInt64Writer : IValueWriter<UInt64>
    {
        public void Write(UInt64 value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public UInt64 Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadUInt64();
        }
    }

    private class DecimalWriter : IValueWriter<Decimal>
    {
        public void Write(Decimal value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Decimal Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadDecimal();
        }
    }

    private class SingleWriter : IValueWriter<Single>
    {
        public void Write(Single value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Single Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadSingle();
        }
    }

    private class DoubleWriter : IValueWriter<Double>
    {
        public void Write(Double value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public Double Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadDouble();
        }
    }

    private class StringWriter : IValueWriter<String>
    {
        public void Write(String value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public String Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadString();
        }
    }

    private class DateTimeWriter : IValueWriter<DateTime>
    {
        public void Write(DateTime value, System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write((int)value.Kind);
            writer.Write(value.Ticks);
        }

        public DateTime Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            var kind = (DateTimeKind)reader.ReadInt32();
            var ticks = reader.ReadInt64();
            return new DateTime(ticks, kind);
        }
    }

    private class BooleanWriter : IValueWriter<Boolean>
    {
        public void Write(bool value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(value);
        }

        public bool Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return reader.ReadBoolean();
        }
    }

    private class GuidWriter : IValueWriter<Guid>
    {
        public void Write(Guid value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            ValueWriter.WriteValue(value.ToByteArray(), writer);
        }

        public Guid Read(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            return new Guid(ValueWriter<byte[]>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1> : IValueWriter<Tuple<T1>>
        where T1 : notnull
    {
        public void Write(Tuple<T1> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
        }

        public Tuple<T1> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2> : IValueWriter<Tuple<T1, T2>>
        where T1 : notnull
        where T2 : notnull
    {
        public void Write(Tuple<T1, T2> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
        }

        public Tuple<T1, T2> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3> : IValueWriter<Tuple<T1, T2, T3>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
    {
        public void Write(Tuple<T1, T2, T3> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
        }

        public Tuple<T1, T2, T3> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3, T4> : IValueWriter<Tuple<T1, T2, T3, T4>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
    {
        public void Write(Tuple<T1, T2, T3, T4> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
        }

        public Tuple<T1, T2, T3, T4> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3, T4, T5> : IValueWriter<Tuple<T1, T2, T3, T4, T5>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
    {
        public void Write(Tuple<T1, T2, T3, T4, T5> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
        }

        public Tuple<T1, T2, T3, T4, T5> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3, T4, T5, T6> : IValueWriter<Tuple<T1, T2, T3, T4, T5, T6>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
    {
        public void Write(Tuple<T1, T2, T3, T4, T5, T6> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
        }

        public Tuple<T1, T2, T3, T4, T5, T6> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3, T4, T5, T6, T7> : IValueWriter<Tuple<T1, T2, T3, T4, T5, T6, T7>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
        where T7 : notnull
    {
        public void Write(Tuple<T1, T2, T3, T4, T5, T6, T7> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
            ValueWriter<T7>.WriteValue(value.Item7, writer);
        }

        public Tuple<T1, T2, T3, T4, T5, T6, T7> Read(BinaryReader reader)
        {
            return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader), ValueWriter<T7>.ReadValue(reader));
        }
    }

    private class TupleWriter<T1, T2, T3, T4, T5, T6, T7, TRest> : IValueWriter<Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
        where T7 : notnull
        where TRest : notnull
    {
        public void Write(Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
            ValueWriter<T7>.WriteValue(value.Item7, writer);
            ValueWriter<TRest>.WriteValue(value.Rest, writer);
        }

        public Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> Read(BinaryReader reader)
        {
            return new Tuple<T1, T2, T3, T4, T5, T6, T7, TRest>(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader), ValueWriter<T7>.ReadValue(reader), ValueWriter<TRest>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1> : IValueWriter<ValueTuple<T1>>
        where T1 : notnull
    {
        public void Write(ValueTuple<T1> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
        }

        public ValueTuple<T1> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2> : IValueWriter<ValueTuple<T1, T2>>
        where T1 : notnull
        where T2 : notnull
    {
        public void Write(ValueTuple<T1, T2> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
        }

        public ValueTuple<T1, T2> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3> : IValueWriter<ValueTuple<T1, T2, T3>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
    {
        public void Write(ValueTuple<T1, T2, T3> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
        }

        public ValueTuple<T1, T2, T3> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3, T4> : IValueWriter<ValueTuple<T1, T2, T3, T4>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
    {
        public void Write(ValueTuple<T1, T2, T3, T4> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
        }

        public ValueTuple<T1, T2, T3, T4> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3, T4, T5> : IValueWriter<ValueTuple<T1, T2, T3, T4, T5>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
    {
        public void Write(ValueTuple<T1, T2, T3, T4, T5> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
        }

        public ValueTuple<T1, T2, T3, T4, T5> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3, T4, T5, T6> : IValueWriter<ValueTuple<T1, T2, T3, T4, T5, T6>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
    {
        public void Write(ValueTuple<T1, T2, T3, T4, T5, T6> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
        }

        public ValueTuple<T1, T2, T3, T4, T5, T6> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3, T4, T5, T6, T7> : IValueWriter<ValueTuple<T1, T2, T3, T4, T5, T6, T7>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
        where T7 : notnull
    {
        public void Write(ValueTuple<T1, T2, T3, T4, T5, T6, T7> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
            ValueWriter<T7>.WriteValue(value.Item7, writer);
        }

        public ValueTuple<T1, T2, T3, T4, T5, T6, T7> Read(BinaryReader reader)
        {
            return ValueTuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader), ValueWriter<T7>.ReadValue(reader));
        }
    }

    private class ValueTupleWriter<T1, T2, T3, T4, T5, T6, T7, TRest> : IValueWriter<ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>>
        where T1 : notnull
        where T2 : notnull
        where T3 : notnull
        where T4 : notnull
        where T5 : notnull
        where T6 : notnull
        where T7 : notnull
        where TRest : struct
    {
        public void Write(ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest> value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(value);
            ValueWriter<T1>.WriteValue(value.Item1, writer);
            ValueWriter<T2>.WriteValue(value.Item2, writer);
            ValueWriter<T3>.WriteValue(value.Item3, writer);
            ValueWriter<T4>.WriteValue(value.Item4, writer);
            ValueWriter<T5>.WriteValue(value.Item5, writer);
            ValueWriter<T6>.WriteValue(value.Item6, writer);
            ValueWriter<T7>.WriteValue(value.Item7, writer);
            ValueWriter<TRest>.WriteValue(value.Rest, writer);
        }

        public ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest> Read(BinaryReader reader)
        {
            return new ValueTuple<T1, T2, T3, T4, T5, T6, T7, TRest>(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader), ValueWriter<T3>.ReadValue(reader), ValueWriter<T4>.ReadValue(reader), ValueWriter<T5>.ReadValue(reader), ValueWriter<T6>.ReadValue(reader), ValueWriter<T7>.ReadValue(reader), ValueWriter<TRest>.ReadValue(reader));
        }
    }

    private class EnumWriter<T, TUnderlying> : IValueWriter<T>
        where T : Enum
        where TUnderlying : notnull
    {
        public T Read(BinaryReader reader)
            => (T)Enum.ToObject(typeof(T), ValueWriter<TUnderlying>.ReadValue(reader));

        public void Write(T value, BinaryWriter writer)
            => ValueWriter<TUnderlying>.WriteValue((TUnderlying)Convert.ChangeType(value, typeof(TUnderlying)), writer);
    }

    private class ByteArrayWriter : IValueWriter<byte[]>
    {
        public byte[] Read(BinaryReader reader)
        {
            var length = reader.Read7BitEncodedInt();
            return reader.ReadBytes(length);
        }

        public void Write(byte[] value, BinaryWriter writer)
        {
            writer.Write7BitEncodedInt(value.Length);
            writer.Write(value);
        }
    }

    private class ArrayWriter<T> : IValueWriter<T[]>
        where T : notnull
    {
        public T[] Read(BinaryReader reader)
        {
            var length = reader.Read7BitEncodedInt();
            var result = new T[length];
            for (int i = 0; i < length; i++)
            {
                result[i] = ValueWriter<T>.ReadValue(reader);
            }

            return result;
        }

        public void Write(T[] value, BinaryWriter writer)
        {
            writer.Write7BitEncodedInt(value.Length);
            foreach (var item in value)
            {
                ValueWriter<T>.WriteValue(item, writer);
            }
        }
    }

    private class ReadOnlyCollectionWriter<T> : IValueWriter<ReadOnlyCollection<T>>
        where T : notnull
    {
        public ReadOnlyCollection<T> Read(BinaryReader reader)
        {
            var length = reader.Read7BitEncodedInt();
            var result = new T[length];
            for (int i = 0; i < length; i++)
            {
                result[i] = ValueWriter<T>.ReadValue(reader);
            }

            return new(result);
        }

        public void Write(ReadOnlyCollection<T> value, BinaryWriter writer)
        {
            writer.Write7BitEncodedInt(value.Count);
            foreach (var item in value)
            {
                ValueWriter<T>.WriteValue(item, writer);
            }
        }
    }

    #endregion

    public static object GetWriter(Type type)
    {
        if (type == typeof(int))
            return new Int32Writer();
        else if (type == typeof(long))
            return new Int64Writer();
        else if (type == typeof(String))
            return new StringWriter();
        else if (type == typeof(Single))
            return new SingleWriter();
        else if (type == typeof(Double))
            return new DoubleWriter();
        else if (type == typeof(SByte))
            return new SByteWriter();
        else if (type == typeof(Int16))
            return new Int16Writer();
        else if (type == typeof(Byte))
            return new ByteWriter();
        else if (type == typeof(UInt16))
            return new UInt16Writer();
        else if (type == typeof(UInt32))
            return new UInt32Writer();
        else if (type == typeof(UInt64))
            return new UInt64Writer();
        else if (type == typeof(Decimal))
            return new DecimalWriter();
        else if (type == typeof(DateTime))
            return new DateTimeWriter();
        else if (type == typeof(Boolean))
            return new BooleanWriter();
        else if (type == typeof(Guid))
            return new GuidWriter();
        else if (type == typeof(byte[]))
            return new ByteArrayWriter();
        else if (type.IsEnum)
            return Activator.CreateInstance(typeof(EnumWriter<,>).MakeGenericType(type, type.GetEnumUnderlyingType()))!;
        else if (type.IsArray)
            return Activator.CreateInstance(typeof(ArrayWriter<>).MakeGenericType(type.GetElementType()!))!;
        else if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            if (definition == typeof(ReadOnlyCollection<>))
            {
                return Activator.CreateInstance(typeof(ReadOnlyCollectionWriter<>).MakeGenericType(type.GetGenericArguments()))!;
            }
            if (definition == typeof(ValueTuple<>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,,,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,,,,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(ValueTuple<,,,,,,,>))
                return Activator.CreateInstance(typeof(ValueTupleWriter<,,,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<>))
                return Activator.CreateInstance(typeof(TupleWriter<>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,>))
                return Activator.CreateInstance(typeof(TupleWriter<,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,,,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,,,,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
            if (definition == typeof(Tuple<,,,,,,,>))
                return Activator.CreateInstance(typeof(TupleWriter<,,,,,,,>).MakeGenericType(type.GetGenericArguments()))!;
        }

        throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0} and the type does not implement IWritable.", type));
    }
}
