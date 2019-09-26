// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Converts a <see cref="BinarySize"/> object from one data type to another. Access this class through the <see cref="TypeDescriptor"/> object.
    /// </summary>
    public class BinarySizeConverter : TypeConverter
    {
        /// <summary>
        /// Determines if this converter can convert an object in the given source type to the native type of the converter.
        /// </summary>
        /// <param name="context">A formatter context. This object can be used to get additional information about the environment this converter is being called from. This may be <see langword="null"/>, so you should always check. Also, properties on the context object may also return <see langword="null"/>. </param>
        /// <param name="sourceType">The type you want to convert from.</param>
        /// <returns><see langword="true"/> if this object can perform the conversion; otherwise, <see langword="false"/>.</returns>
        public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
        {
            if( sourceType == typeof(string) || sourceType == typeof(long) || sourceType == typeof(ulong) || sourceType == typeof(int) || sourceType == typeof(uint) ||
                sourceType == typeof(short) || sourceType == typeof(ushort) || sourceType == typeof(byte) || sourceType == typeof(sbyte) ||
                sourceType == typeof(decimal) || sourceType == typeof(float) || sourceType == typeof(int) || sourceType == typeof(double) )
                return true;
            else
                return base.CanConvertFrom(context, sourceType);
        }

        /// <summary>
        /// Gets a value indicating whether this converter can convert an object to the given destination type using the context. 
        /// </summary>
        /// <param name="context">An <see cref="ITypeDescriptorContext"/> object that provides a format context.</param>
        /// <param name="destinationType">A <see cref="Type"/> object that represents the type you want to convert to.</param>
        /// <returns><see langword="true"/> if this object can perform the conversion; otherwise, <see langword="false"/>.</returns>
        public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
        {
            if( destinationType == typeof(string) || destinationType == typeof(long) || destinationType == typeof(ulong) || destinationType == typeof(int) || destinationType == typeof(uint) ||
                destinationType == typeof(short) || destinationType == typeof(ushort) || destinationType == typeof(byte) || destinationType == typeof(sbyte) ||
                destinationType == typeof(decimal) || destinationType == typeof(float) || destinationType == typeof(int) || destinationType == typeof(double) )
                return true;
            else
                return base.CanConvertTo(context, destinationType);
        }

        /// <summary>
        /// Converts the specified object to a <see cref="BinarySize"/> object.
        /// </summary>
        /// <param name="context">A formatter context. This object can be used to get additional information about the environment this converter is being called from. This may be <see langword="null"/>, so you should always check. Also, properties on the context object may also return <see langword="null"/>. </param>
        /// <param name="culture">An object that contains culture specific information, such as the language, calendar, and cultural conventions associated with a specific culture. It is based on the RFC 1766 standard.</param>
        /// <param name="value">The object to convert.</param>
        /// <returns>The converted object.</returns>
        public override object ConvertFrom(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value)
        {
            string stringValue = value as string;
            if( stringValue != null )
                return BinarySize.Parse(stringValue, culture);
            else if( value is byte )
                return new BinarySize((byte)value);
            else if( value is sbyte )
                return new BinarySize((sbyte)value);
            else if( value is short )
                return new BinarySize((short)value);
            else if( value is ushort )
                return new BinarySize((ushort)value);
            else if( value is int )
                return new BinarySize((int)value);
            else if( value is uint )
                return new BinarySize((uint)value);
            else if( value is long )
                return new BinarySize((long)value);
            else if( value is ulong )
                return new BinarySize((ulong)value);
            else if( value is decimal )
                return (BinarySize)(decimal)value;
            else if( value is float )
                return (BinarySize)(float)value;
            else if( value is double )
                return (BinarySize)(double)value;
            else
                return base.ConvertFrom(context, culture, value);
        }

        /// <summary>
        /// Converts the specified object to the specified type. 
        /// </summary>
        /// <param name="context">A formatter context. This object can be used to get additional information about the environment this converter is being called from. This may be <see langword="null"/>, so you should always check. Also, properties on the context object may also return <see langword="null"/>. </param>
        /// <param name="culture">An object that contains culture specific information, such as the language, calendar, and cultural conventions associated with a specific culture. It is based on the RFC 1766 standard.</param>
        /// <param name="value">The object to convert.</param>
        /// <param name="destinationType">The type to convert the object to.</param>
        /// <returns>The converted object.</returns>
        public override object ConvertTo(ITypeDescriptorContext context, System.Globalization.CultureInfo culture, object value, Type destinationType)
        {
            if( !(value is BinarySize) )
                throw new ArgumentException("Cannot convert argument: incorrect type.", nameof(value));

            BinarySize realValue = (BinarySize)value;
            if( destinationType == typeof(string) )
                return realValue.ToString(culture);
            else if( destinationType == typeof(byte) )
                return (byte)realValue;
            else if( destinationType == typeof(sbyte) )
                return (sbyte)realValue;
            else if( destinationType == typeof(short) )
                return (short)realValue;
            else if( destinationType == typeof(ushort) )
                return (ushort)realValue;
            else if( destinationType == typeof(int) )
                return (int)realValue;
            else if( destinationType == typeof(uint) )
                return (uint)realValue;
            else if( destinationType == typeof(long) )
                return (long)realValue;
            else if( destinationType == typeof(ulong) )
                return (ulong)realValue;
            else if( destinationType == typeof(decimal) )
                return (decimal)realValue;
            else if( destinationType == typeof(float) )
                return (float)realValue;
            else if( destinationType == typeof(double) )
                return (double)realValue;
            else
                return base.ConvertTo(context, culture, value, destinationType);
        }
    }
}
