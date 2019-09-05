// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;
using System.Runtime.Serialization;

namespace Ookii.Jumbo.IO
{
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
        public static object GetWriter(Type type)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( type.GetInterfaces().Contains(typeof(IWritable)) )
                return null;
            ValueWriterAttribute attribute = (ValueWriterAttribute)Attribute.GetCustomAttribute(type, typeof(ValueWriterAttribute));
            if( attribute != null && !string.IsNullOrEmpty(attribute.ValueWriterTypeName) )
            {
                Type writerType = Type.GetType(attribute.ValueWriterTypeName, true);
                return Activator.CreateInstance(writerType);
            }

            return DefaultValueWriter.GetWriter(type);
        }
    }

    /// <summary>
    /// Provides access to <see cref="IValueWriter{T}"/> implementations for various basic framework types and types that specify the <see cref="ValueWriterAttribute"/> attribute.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   If you attempt to access this class for a type <typeparam name="T" /> that does not implement <see cref="IWritable"/> and that does not have a implementation of <see cref="IValueWriter{T}"/>,
    ///   an <see cref="NotSupportedException"/> is thrown by the static type initializer of the <see cref="ValueWriter{T}"/> class.
    /// </para>
    /// <para>
    ///   Built-in value writers are provided for the following types: <see cref="SByte"/>, <see cref="Int16"/>, <see cref="Int32"/>, <see cref="Int64"/>, <see cref="Byte"/>, <see cref="UInt16"/>,
    ///   <see cref="UInt32"/>, <see cref="UInt64"/>, <see cref="Decimal"/>, <see cref="Single"/>, <see cref="Double"/>, <see cref="String"/>, <see cref="DateTime"/>, <see cref="Boolean"/>,
    ///   <see cref="Tuple{T1}"/>, <see cref="Tuple{T1, T2}"/>, <see cref="Tuple{T1, T2, T3}"/>, <see cref="Tuple{T1, T2, T3, T4}"/>, <see cref="Tuple{T1, T2, T3, T4, T5}"/>, <see cref="Tuple{T1, T2, T3, T4, T5, T6}"/>,
    ///   <see cref="Tuple{T1, T2, T3, T4, T5, T6, T7}"/> and <see cref="Tuple{T1, T2, T3, T4, T5, T6, T7, TRest}"/>.
    /// </para>
    /// <para>
    ///   Although value writers support tuples, there may be negative performance implications from using them. All the .Net Framework tuple types are reference types, but they are
    ///   read-only and therefore do not support record reuse. This means that for every record, a new tuple object must be created, as well as new instances of any of the
    ///   tuple's items that are reference types, even in situations where record reuse is allowed.
    /// </para>
    /// <para>
    ///   Instead of using tuples, it is recommended to create a custom class that implements <see cref="IWritable"/> for your records.
    /// </para>
    /// <para>
    ///   The <see cref="KeyValuePair{TKey, TValue}"/> structure is not supported by the value writer. Please use the <see cref="Pair{TKey, TValue}"/> class instead, which provides
    ///   additional functionality for key/value pairs required by Jumbo.
    /// </para>
    /// </remarks>
    public static class ValueWriter<T>
    {
        private static readonly IValueWriter<T> _writer = (IValueWriter<T>)ValueWriter.GetWriter(typeof(T));

        /// <summary>
        /// Gets the writer for the type, or <see langword="null"/> if it implements <see cref="IWritable"/>.
        /// </summary>
        /// <value>
        /// An implementation of <see cref="IValueWriter{T}"/>, or <see langword="null"/> if it implements <see cref="IWritable"/>.
        /// </value>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static IValueWriter<T> Writer
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static void WriteValue(T value, BinaryWriter writer)
        {
            if( _writer == null )
                ((IWritable)value).Write(writer);
            else
                _writer.Write(value, writer);
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
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes")]
        public static T ReadValue(BinaryReader reader)
        {
            if( _writer == null )
            {
                T result = (T)FormatterServices.GetUninitializedObject(typeof(T));
                ((IWritable)result).Read(reader);
                return result;
            }
            else
                return _writer.Read(reader);
        }
    }
}
