// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Provides raw comparers for built-in framework types.
/// </summary>
[global::System.Runtime.CompilerServices.CompilerGenerated]
static class DefaultRawComparer
{
    #region Nested types

    private sealed class SByteComparer : IRawComparer<SByte>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = (SByte)buffer1[offset1];
            var value2 = (SByte)buffer2[offset2];
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(SByte x, SByte y)
        {
            return Comparer<SByte>.Default.Compare(x, y);
        }
    }

    private sealed class ByteComparer : IRawComparer<Byte>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = buffer1[offset1];
            var value2 = buffer2[offset2];
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Byte x, Byte y)
        {
            return Comparer<Byte>.Default.Compare(x, y);
        }
    }

    private sealed class Int16Comparer : IRawComparer<Int16>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToInt16(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToInt16(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Int16 x, Int16 y)
        {
            return Comparer<Int16>.Default.Compare(x, y);
        }
    }

    private sealed class UInt16Comparer : IRawComparer<UInt16>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToUInt16(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToUInt16(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(UInt16 x, UInt16 y)
        {
            return Comparer<UInt16>.Default.Compare(x, y);
        }
    }

    private sealed class Int32Comparer : IRawComparer<Int32>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToInt32(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToInt32(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Int32 x, Int32 y)
        {
            return Comparer<Int32>.Default.Compare(x, y);
        }
    }

    private sealed class UInt32Comparer : IRawComparer<UInt32>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToUInt32(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToUInt32(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(UInt32 x, UInt32 y)
        {
            return Comparer<UInt32>.Default.Compare(x, y);
        }
    }

    private sealed class Int64Comparer : IRawComparer<Int64>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToInt64(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToInt64(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Int64 x, Int64 y)
        {
            return Comparer<Int64>.Default.Compare(x, y);
        }
    }

    private sealed class UInt64Comparer : IRawComparer<UInt64>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToUInt64(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToUInt64(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(UInt64 x, UInt64 y)
        {
            return Comparer<UInt64>.Default.Compare(x, y);
        }
    }

    private sealed class DecimalComparer : IRawComparer<Decimal>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToDecimal(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToDecimal(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Decimal x, Decimal y)
        {
            return Comparer<Decimal>.Default.Compare(x, y);
        }
    }

    private sealed class SingleComparer : IRawComparer<Single>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToSingle(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToSingle(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Single x, Single y)
        {
            return Comparer<Single>.Default.Compare(x, y);
        }
    }

    private sealed class DoubleComparer : IRawComparer<Double>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToDouble(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToDouble(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(Double x, Double y)
        {
            return Comparer<Double>.Default.Compare(x, y);
        }
    }

    private sealed class DateTimeComparer : IRawComparer<DateTime>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            var value1 = LittleEndianBitConverter.ToDateTime(buffer1, offset1);
            var value2 = LittleEndianBitConverter.ToDateTime(buffer2, offset2);
            return value1 < value2 ? -1 : (value1 == value2 ? 0 : 1);
        }

        public int Compare(DateTime x, DateTime y)
        {
            return Comparer<DateTime>.Default.Compare(x, y);
        }
    }

    private sealed class StringRawComparer : IRawComparer<string>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            return RawComparerHelper.CompareBytesWith7BitEncodedLength(buffer1, offset1, count1, buffer2, offset2, count2);
        }

        public int Compare(string? x, string? y)
        {
            return StringComparer.Ordinal.Compare(x, y);
        }
    }

    #endregion

    public static object? GetComparer(Type type)
    {
        if (type == typeof(SByte))
        {
            return new SByteComparer();
        }
        else if (type == typeof(Byte))
        {
            return new ByteComparer();
        }
        else if (type == typeof(Int16))
        {
            return new Int16Comparer();
        }
        else if (type == typeof(UInt16))
        {
            return new UInt16Comparer();
        }
        else if (type == typeof(Int32))
        {
            return new Int32Comparer();
        }
        else if (type == typeof(UInt32))
        {
            return new UInt32Comparer();
        }
        else if (type == typeof(Int64))
        {
            return new Int64Comparer();
        }
        else if (type == typeof(UInt64))
        {
            return new UInt64Comparer();
        }
        else if (type == typeof(Decimal))
        {
            return new DecimalComparer();
        }
        else if (type == typeof(Single))
        {
            return new SingleComparer();
        }
        else if (type == typeof(Double))
        {
            return new DoubleComparer();
        }
        else if (type == typeof(DateTime))
        {
            return new DateTimeComparer();
        }
        else if (type == typeof(String))
        {
            return new StringRawComparer();
        }

        return null;
    }
}
