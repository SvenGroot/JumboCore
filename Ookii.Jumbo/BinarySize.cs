// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;
using System.ComponentModel;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides formatting, parsing and scaling for a value using binary units (e.g. MB).
    /// </summary>
    [TypeConverter(typeof(BinarySizeConverter))]
    public struct BinarySize : IEquatable<BinarySize>, IComparable<BinarySize>, IComparable, IFormattable, IConvertible
    {
        /// <summary>
        /// The size of a byte, 1 byte.
        /// </summary>
        public const long Byte = 1L;
        /// <summary>
        /// The size of a kilobyte, 1024 bytes.
        /// </summary>
        public const long Kilobyte = 1024L;
        /// <summary>
        /// The size of a megabyte, 1048576 bytes.
        /// </summary>
        public const long Megabyte = 1024L * 1024L;
        /// <summary>
        /// The size of a gigabyte, 1073741824 bytes.
        /// </summary>
        public const long Gigabyte = 1024L * 1024L * 1024L;
        /// <summary>
        /// The size of a TeraByte, 1099511627776 bytes.
        /// </summary>
        public const long Terabyte = 1024L * 1024L * 1024L * 1024L;
        /// <summary>
        /// The size of a PetaByte, 1125899906842624 bytes.
        /// </summary>
        public const long Petabyte = 1024L * 1024L * 1024L * 1024L * 1024L;


        private static readonly char[] _numbers = new[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        private static readonly BinarySize _zero = new BinarySize();
        private readonly long _value;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        public BinarySize(int value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        [CLSCompliant(false)]
        public BinarySize(uint value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        public BinarySize(long value)
        {
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySize"/> structure with the specified value.
        /// </summary>
        /// <param name="value">The size, in bytes.</param>
        [CLSCompliant(false)]
        public BinarySize(ulong value)
        {
            checked
            {
                _value = (long)value;
            }
        }

        /// <summary>
        /// Gets the value of this instance, in bytes.
        /// </summary>
        /// <value>
        /// The value of this instance, in bytes.
        /// </value>
        public long Value
        {
            get { return _value; }
        }

        /// <summary>
        /// Gets the value of this instance in kilobytes.
        /// </summary>
        /// <value>
        /// The value of this instance in whole and fractional kilobytes (<see cref="Value"/> / <see cref="Kilobyte"/>).
        /// </value>
        public double InKilobytes
        {
            get { return _value / (double)Kilobyte; }
        }

        /// <summary>
        /// Gets the value of this instance in megabytes.
        /// </summary>
        /// <value>
        /// The value of this instance in whole and fractional megabytes (<see cref="Value"/> / <see cref="Megabyte"/>)
        /// </value>
        public double InMegabytes
        {
            get { return _value / (double)Megabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in gigabytes.
        /// </summary>
        /// <value>
        /// The value of this instance in whole and fractional gigabytes (<see cref="Value"/> / <see cref="Gigabyte"/>)
        /// </value>
        public double InGigabytes
        {
            get { return _value / (double)Gigabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in terabytes.
        /// </summary>
        /// <value>
        /// The value of this instance in whole and fractional terabytes (<see cref="Value"/> / <see cref="Terabyte"/>)
        /// </value>
        public double InTerabytes
        {
            get { return _value / (double)Terabyte; }
        }

        /// <summary>
        /// Gets the value of this instance in petabytes.
        /// </summary>
        /// <value>
        /// The value of this instance in whole and fractional petabytes (<see cref="Value"/> / <see cref="Petabyte"/>)
        /// </value>
        public double InPetabytes
        {
            get { return _value / (double)Petabyte; }
        }

        /// <summary>
        /// Gets a zero-valued <see cref="BinarySize"/> instance.
        /// </summary>
        /// <value>A <see cref="BinarySize"/> instance with <see cref="Value"/> set to zero.</value>
        public static BinarySize Zero
        {
            get { return _zero; }
        }

        /// <summary>
        /// Returns a new <see cref="BinarySize"/> object whose value is the sum of the specified <see cref="BinarySize"/> object and this instance.
        /// </summary>
        /// <param name="value">The value to add.</param>
        /// <returns>A new object that represents the value of this instance plus <paramref name="value"/>.</returns>
        public BinarySize Add(BinarySize value)
        {
            return Value + value.Value;
        }

        /// <summary>
        /// Returns a new <see cref="BinarySize"/> object whose value is the difference of the specified <see cref="BinarySize"/> object and this instance.
        /// </summary>
        /// <param name="value">The value to subtract.</param>
        /// <returns>A new object that represents the value of this instance minus <paramref name="value"/>.</returns>
        public BinarySize Subtract(BinarySize value)
        {
            return Value - value.Value;
        }

        /// <summary>
        /// Returns a new <see cref="BinarySize"/> object whose value is the product of the specified <see cref="Int64"/> object and this instance.
        /// </summary>
        /// <param name="value">The value to multiply by.</param>
        /// <returns>A new object that represents the value of this instance times <paramref name="value"/>.</returns>
        public BinarySize Multiply(long value)
        {
            return Value * value;
        }

        /// <summary>
        /// Returns a new <see cref="BinarySize"/> object whose value is the division of the specified <see cref="Int64"/> object and this instance.
        /// </summary>
        /// <param name="value">The value to divide by.</param>
        /// <returns>A new object that represents the value of this instance divided by <paramref name="value"/>.</returns>
        public BinarySize Divide(long value)
        {
            return Value / value;
        }

        /// <summary>
        /// Returns a new <see cref="BinarySize"/> object whose value is the negation of this instance.
        /// </summary>
        /// <returns>A new object that represents the negated value of this instance.</returns>
        public BinarySize Negate()
        {
            return -Value;
        }

        /// <summary>
        /// Converts the string representation of a byte size in a specified culture-specific format into a <see cref="BinarySize"/> structure.
        /// </summary>
        /// <param name="value">A string containing a number to convert. This string may use a suffix indicating a binary multiple (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P).</param>
        /// <param name="provider">An <see cref="IFormatProvider"/> that supplies culture-specific formatting information about <paramref name="value" />. May be <see langword="null"/> to use the current culture.</param>
        /// <returns>A <see cref="BinarySize"/> instance that is the equivalent of <paramref name="value"/>.</returns>
        public static BinarySize Parse(string value, IFormatProvider provider)
        {
            if( value == null )
                throw new ArgumentNullException("value");
            if( value.Length == 0 )
                return new BinarySize();

            string suffix = GetAndRemoveSuffix(ref value);
            Decimal size = Decimal.Parse(value, provider);
            if( suffix != null )
                size *= GetUnitScalingFactor(suffix);

            checked
            {
                return new BinarySize((long)size);
            }
        }

        /// <summary>
        /// Converts the string representation of a byte size into a <see cref="BinarySize"/> structure.
        /// </summary>
        /// <param name="value">A string containing a number to convert. This string may use a suffix indicating a binary multiple (B, KB, KiB, K, MB, MiB, M, GB, GiB, G, TB, TiB, T, PB, PiB, or P).</param>
        /// <returns>A <see cref="BinarySize"/> instance that is the equivalent of <paramref name="value"/>.</returns>
        public static BinarySize Parse(string value)
        {
            return Parse(value, CultureInfo.CurrentCulture);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="formatProvider">The format provider.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   The value of <paramref name="format"/> must be a string containing a numeric format string followed by a binary unit, or either one of both. If no numeric
        ///   format is present, the default is used. If no binary unit is specified, the raw value in bytes is used.
        /// </para>
        /// <para>
        ///   The first character of the binary suffix indicates the scaling factor. This can be one of the normal binary prefixes K, M, G, T, or P. The value A (auto) indicates that
        ///   the scaling factor should be automatically determined as the largest factor in which this value can be precisely represented with no decimals. The value S (short)
        ///   indicates that the scaling factor should be automatically determined as the largest possible scaling factor in which this value can be represented with the scaled
        ///   value being at least 1. Using S may lead to rounding so while this is appropriate for some display scenarios, it is not appropriate if the precise value must be preserved.
        /// </para>
        /// <para>
        ///   The binary prefix can be followed by either B or iB, which will be included in the unit of the output.
        /// </para>
        /// <para>
        ///   The casing of the binary unit will be preserved as in the format string. Any whitespace that surrounding the binary unit will be preserved.
        /// </para>
        /// </remarks>
        public string ToString(string format, IFormatProvider formatProvider)
        {
            return BinarySizeFormatter.Format(this, format, formatProvider);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="format">The format.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   The value of <paramref name="format"/> must be a string containing a numeric format string followed by a binary unit, or either one of both. If no numeric
        ///   format is present, the default is used. If no binary unit is specified, the raw value in bytes is used.
        /// </para>
        /// <para>
        ///   The first character of the binary suffix indicates the scaling factor. This can be one of the normal binary prefixes K, M, G, T, or P. The value A (auto) indicates that
        ///   the scaling factor should be automatically determined as the largest factor in which this value can be precisely represented with no decimals. The value S (short)
        ///   indicates that the scaling factor should be automatically determined as the largest possible scaling factor in which this value can be represented with the scaled
        ///   value being at least 1. Using S may lead to rounding so while this is appropriate for some display scenarios, it is not appropriate if the precise value must be preserved.
        /// </para>
        /// <para>
        ///   The binary prefix can be followed by either B or iB to indicate the the unit formatting.
        /// </para>
        /// <para>
        ///   The casing of the binary unit will be preserved as in the format string. Any whitespace that surrounding the binary unit will be preserved.
        /// </para>
        /// </remarks>
        public string ToString(string format)
        {
            return BinarySizeFormatter.Format(this, format, null);
        }


        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <param name="provider">The provider.</param>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public string ToString(IFormatProvider provider)
        {
            return BinarySizeFormatter.Format(this, null, provider);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return BinarySizeFormatter.Format(this, null, null);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified object.
        /// </summary>
        /// <param name="obj">The object to compare to this instance.</param>
        /// <returns><see langword="true"/> if <paramref name="obj"/> has the same value as this instance; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            if( obj is BinarySize )
                return Equals((BinarySize)obj);
            else
                return false;
        }

        /// <summary>
        /// Returns the hash code for this <see cref="BinarySize"/>.
        /// </summary>
        /// <returns>The hash code for this <see cref="BinarySize"/>.</returns>
        public override int GetHashCode()
        {
            return Value.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified <see cref="BinarySize"/> values are the same.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(BinarySize left, BinarySize right)
        {
            return left.Value == right.Value;
        }

        /// <summary>
        /// Determines whether two specified <see cref="BinarySize"/> values are different.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(BinarySize left, BinarySize right)
        {
            return left.Value != right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="BinarySize"/> is less than another <see cref="BinarySize"/>.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(BinarySize left, BinarySize right)
        {
            return left.Value < right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="BinarySize"/> is less than or equal to another <see cref="BinarySize"/>.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than or equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <=(BinarySize left, BinarySize right)
        {
            return left.Value <= right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="BinarySize"/> is greater than another <see cref="BinarySize"/>.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(BinarySize left, BinarySize right)
        {
            return left.Value > right.Value;
        }

        /// <summary>
        /// Returns a value indicating whether a specified <see cref="BinarySize"/> is greater than or equal to another <see cref="BinarySize"/>.
        /// </summary>
        /// <param name="left">A <see cref="BinarySize"/>.</param>
        /// <param name="right">A <see cref="BinarySize"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than or equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >=(BinarySize left, BinarySize right)
        {
            return left.Value >= right.Value;
        }

        /// <summary>
        /// Implements the operator +.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static BinarySize operator +(BinarySize left, BinarySize right)
        {
            return left.Value + right.Value;
        }

        /// <summary>
        /// Implements the operator -.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static BinarySize operator -(BinarySize left, BinarySize right)
        {
            return left.Value - right.Value;
        }

        /// <summary>
        /// Implements the operator *.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static BinarySize operator *(BinarySize left, long right)
        {
            return left.Value * right;
        }

        /// <summary>
        /// Implements the operator /.
        /// </summary>
        /// <param name="left">The left operand.</param>
        /// <param name="right">The right operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static BinarySize operator /(BinarySize left, long right)
        {
            return left.Value / right;
        }

        /// <summary>
        /// Implements the unary operator -.
        /// </summary>
        /// <param name="value">The operand.</param>
        /// <returns>
        /// The result of the operator.
        /// </returns>
        public static BinarySize operator -(BinarySize value)
        {
            return -value.Value;
        }

        #region Conversion operators

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Byte"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator byte(BinarySize value)
        {
            checked
            {
                return (byte)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.SByte"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator sbyte(BinarySize value)
        {
            checked
            {
                return (sbyte)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Int16"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator short(BinarySize value)
        {
            checked
            {
                return (short)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.UInt16"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator ushort(BinarySize value)
        {
            checked
            {
                return (ushort)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Int32"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator int(BinarySize value)
        {
            checked
            {
                return (int)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.UInt32"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static explicit operator uint(BinarySize value)
        {
            checked
            {
                return (uint)value.Value;
            }
        }

        /// <summary>
        /// Converts the specified <see cref="BinarySize"/> to an <see cref="Int64"/>.
        /// </summary>
        /// <param name="value">The <see cref="BinarySize"/> to convert.</param>
        /// <returns>The value of the <see cref="BinarySize"/> in bytes.</returns>
        public static explicit operator long(BinarySize value)
        {
            return value.Value;
        }

        /// <summary>
        /// Converts the specified <see cref="BinarySize"/> to an <see cref="UInt64"/>.
        /// </summary>
        /// <param name="value">The <see cref="BinarySize"/> to convert.</param>
        /// <returns>The value of the <see cref="BinarySize"/> in bytes.</returns>
        [CLSCompliant(false)]
        public static explicit operator ulong(BinarySize value)
        {
            checked
            {
                return (ulong)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Decimal"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator decimal(BinarySize value)
        {
            checked
            {
                return (decimal)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Single"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator float(BinarySize value)
        {
            checked
            {
                return (float)value.Value;
            }
        }

        /// <summary>
        /// Performs an explicit conversion from <see cref="Ookii.Jumbo.BinarySize"/> to <see cref="System.Double"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static explicit operator double(BinarySize value)
        {
            checked
            {
                return (double)value.Value;
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Byte"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(byte value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.SByte"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BinarySize(sbyte value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int16"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(short value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt16"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BinarySize(ushort value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int32"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(int value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt32"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BinarySize(uint value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Int64"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(long value)
        {
            return new BinarySize(value);
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.UInt64"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        [CLSCompliant(false)]
        public static implicit operator BinarySize(ulong value)
        {
            checked
            {
                return new BinarySize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Decimal"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(decimal value)
        {
            checked
            {
                return new BinarySize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Single"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(float value)
        {
            checked
            {
                return new BinarySize((long)value);
            }
        }

        /// <summary>
        /// Performs an implicit conversion from <see cref="System.Double"/> to <see cref="Ookii.Jumbo.BinarySize"/>.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        /// The result of the conversion.
        /// </returns>
        public static implicit operator BinarySize(double value)
        {
            checked
            {
                return new BinarySize((long)value);
            }
        }

        #endregion


        #region IEquatable<ByteSize> Members

        /// <summary>
        /// Returns a value indicating whether this instance is equal to a specified <see cref="BinarySize"/> value.
        /// </summary>
        /// <param name="other">The <see cref="BinarySize"/> value to compare to this instance.</param>
        /// <returns><see langword="true"/> if <paramref name="other"/> has the same value as this instance; otherwise, <see langword="false"/>.</returns>
        public bool Equals(BinarySize other)
        {
            return object.Equals(Value, other.Value);
        }

        #endregion

        #region IComparable<ByteSize> Members

        /// <summary>
        /// Compares this instance to a specified <see cref="BinarySize"/> and returns an indication of their relative values.
        /// </summary>
        /// <param name="other">A <see cref="BinarySize"/> to compare.</param>
        /// <returns>Less than zero if this instance is less than <paramref name="other"/>, zero if this instance is equal to <paramref name="other"/>, or greater than zero if this instance is greater than <paramref name="other"/>.</returns>
        public int CompareTo(BinarySize other)
        {
            return Value.CompareTo(other.Value);
        }

        #endregion

        #region IComparable Members

        /// <summary>
        /// Compares this instance to a specified object and returns an indication of their relative values.
        /// </summary>
        /// <param name="obj">An object to compare.</param>
        /// <returns>Less than zero if this instance is less than <paramref name="obj"/>, zero if this instance is equal to <paramref name="obj"/>, or greater than zero if this instance is greater than <paramref name="obj"/> or <paramref name="obj"/> is <see langword="null"/>.</returns>
        public int CompareTo(object obj)
        {
            if( obj == null )
                return 1;
            else if( obj is BinarySize )
                return CompareTo((BinarySize)obj);
            else
                throw new ArgumentException("The specified value is not a ByteSize.", "obj");
        }

        #endregion

        #region IConvertable members

        TypeCode IConvertible.GetTypeCode()
        {
            return TypeCode.Object;
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return Convert.ToBoolean(Value, provider);
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return Convert.ToByte(Value, provider);
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            return Convert.ToChar(Value, provider);
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            return Convert.ToDateTime(Value, provider);
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Convert.ToDecimal(Value, provider);
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return Convert.ToDouble(Value, provider);
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return Convert.ToInt16(Value, provider);
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return Convert.ToInt32(Value, provider);
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return Convert.ToInt64(Value, provider);
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return Convert.ToSByte(Value, provider);
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return Convert.ToSingle(Value, provider);
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            if( conversionType == typeof(string) )
                return ToString(provider);
            else
                return Convert.ChangeType(Value, conversionType, provider);
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return Convert.ToUInt16(Value, provider);
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return Convert.ToUInt32(Value, provider);
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return Convert.ToUInt64(Value, provider);
        }

        #endregion

        internal static long GetUnitScalingFactor(string unit)
        {
            switch( unit.ToUpperInvariant() )
            {
            case "B":
                return 1;
            case "KB":
            case "KIB":
            case "K":
                return Kilobyte;
            case "MB":
            case "MIB":
            case "M":
                return Megabyte;
            case "GB":
            case "GIB":
            case "G":
                return Gigabyte;
            case "TB":
            case "TIB":
            case "T":
                return Terabyte;
            case "PB":
            case "PIB":
            case "P":
                return Petabyte;
            default:
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Unrecognized unit {0}.", unit), "unit");
            }
        }

        private static string GetAndRemoveSuffix(ref string value)
        {
            int lastNumber = value.LastIndexOfAny(_numbers);
            if( lastNumber == value.Length - 1 )
                return null;
            else
            {
                string suffix = value.Substring(lastNumber + 1);
                value = value.Substring(0, lastNumber + 1);
                return suffix.Trim();
            }
        }
    }
}
