// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.IO
{
    static class DefaultValueWriter
    {
        #region Nested types

        private class SByteWriter : IValueWriter<SByte>
        {
            public void Write(SByte value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public SByte Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadSByte();
            }
        }

        private class Int16Writer : IValueWriter<Int16>
        {
            public void Write(Int16 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Int16 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt16();
            }
        }

        private class Int32Writer : IValueWriter<Int32>
        {
            public void Write(int value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public int Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt32();
            }
        }

        private class Int64Writer : IValueWriter<Int64>
        {
            public void Write(Int64 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Int64 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadInt64();
            }
        }

        private class ByteWriter : IValueWriter<Byte>
        {
            public void Write(Byte value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Byte Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadByte();
            }
        }

        private class UInt16Writer : IValueWriter<UInt16>
        {
            public void Write(UInt16 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt16 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt16();
            }
        }

        private class UInt32Writer : IValueWriter<UInt32>
        {
            public void Write(UInt32 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt32 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt32();
            }
        }

        private class UInt64Writer : IValueWriter<UInt64>
        {
            public void Write(UInt64 value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public UInt64 Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadUInt64();
            }
        }

        private class DecimalWriter : IValueWriter<Decimal>
        {
            public void Write(Decimal value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Decimal Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadDecimal();
            }
        }

        private class SingleWriter : IValueWriter<Single>
        {
            public void Write(Single value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Single Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadSingle();
            }
        }

        private class DoubleWriter : IValueWriter<Double>
        {
            public void Write(Double value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public Double Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadDouble();
            }
        }

        private class StringWriter : IValueWriter<String>
        {
            public void Write(String value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public String Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadString();
            }
        }

        private class DateTimeWriter : IValueWriter<DateTime>
        {
            public void Write(DateTime value, System.IO.BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write((int)value.Kind);
                writer.Write(value.Ticks);
            }

            public DateTime Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                DateTimeKind kind = (DateTimeKind)reader.ReadInt32();
                long ticks = reader.ReadInt64();
                return new DateTime(ticks, kind);
            }
        }

        private class BooleanWriter : IValueWriter<Boolean>
        {
            public void Write(bool value, BinaryWriter writer)
            {
                if( writer == null )
                    throw new ArgumentNullException("writer");
                writer.Write(value);
            }

            public bool Read(BinaryReader reader)
            {
                if( reader == null )
                    throw new ArgumentNullException("reader");
                return reader.ReadBoolean();
            }
        }

        private class TupleWriter<T1> : IValueWriter<Tuple<T1>>
        {
            public void Write(Tuple<T1> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
                ValueWriter<T1>.WriteValue(value.Item1, writer);
            }

            public Tuple<T1> Read(BinaryReader reader)
            {
                return Tuple.Create(ValueWriter<T1>.ReadValue(reader));
            }
        }

        private class TupleWriter<T1, T2> : IValueWriter<Tuple<T1, T2>>
        {
            public void Write(Tuple<T1, T2> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
                ValueWriter<T1>.WriteValue(value.Item1, writer);
                ValueWriter<T2>.WriteValue(value.Item2, writer);
            }

            public Tuple<T1, T2> Read(BinaryReader reader)
            {
                return Tuple.Create(ValueWriter<T1>.ReadValue(reader), ValueWriter<T2>.ReadValue(reader));
            }
        }

        private class TupleWriter<T1, T2, T3> : IValueWriter<Tuple<T1, T2, T3>>
        {
            public void Write(Tuple<T1, T2, T3> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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
        {
            public void Write(Tuple<T1, T2, T3, T4> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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
        {
            public void Write(Tuple<T1, T2, T3, T4, T5> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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
        {
            public void Write(Tuple<T1, T2, T3, T4, T5, T6> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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
        {
            public void Write(Tuple<T1, T2, T3, T4, T5, T6, T7> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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
        {
            public void Write(Tuple<T1, T2, T3, T4, T5, T6, T7, TRest> value, BinaryWriter writer)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
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

        #endregion

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1506:AvoidExcessiveClassCoupling")]
        public static object GetWriter(Type type)
        {
            if( type == typeof(int) )
                return new Int32Writer();
            else if( type == typeof(long) )
                return new Int64Writer();
            else if( type == typeof(String) )
                return new StringWriter();
            else if( type == typeof(Single) )
                return new SingleWriter();
            else if( type == typeof(Double) )
                return new DoubleWriter();
            else if( type == typeof(SByte) )
                return new SByteWriter();
            else if( type == typeof(Int16) )
                return new Int16Writer();
            else if( type == typeof(Byte) )
                return new ByteWriter();
            else if( type == typeof(UInt16) )
                return new UInt16Writer();
            else if( type == typeof(UInt32) )
                return new UInt32Writer();
            else if( type == typeof(UInt64) )
                return new UInt64Writer();
            else if( type == typeof(Decimal) )
                return new DecimalWriter();
            else if( type == typeof(DateTime) )
                return new DateTimeWriter();
            else if( type == typeof(Boolean) )
                return new BooleanWriter();
            else if( type.IsGenericType )
            {
                Type definition = type.GetGenericTypeDefinition();
                if( definition == typeof(Tuple<>) )
                    return Activator.CreateInstance(typeof(TupleWriter<>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,,,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,,,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,,,,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,,,,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,,,,,>).MakeGenericType(type.GetGenericArguments()));
                if( definition == typeof(Tuple<,,,,,,,>) )
                    return Activator.CreateInstance(typeof(TupleWriter<,,,,,,,>).MakeGenericType(type.GetGenericArguments()));
                else
                    throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0} and the type does not implement IWritable.", type));
            }
            else
                throw new NotSupportedException(string.Format(System.Globalization.CultureInfo.InvariantCulture, "Could not find the writer for type {0} and the type does not implement IWritable.", type));

        }
    }
}
