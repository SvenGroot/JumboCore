// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for binary serialization of types that don't support <see cref="IWritable"/>.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   Jumbo provides its own simple, light-weight serialization protocol that is primarily
    ///   used by the <see cref="BinaryRecordReader{T}"/> and <see cref="BinaryRecordWriter{T}"/>
    ///   classes (although it is also used in a few other places).
    /// </para>
    /// <para>
    ///   Normally, classes support this protocol by implementing the <see cref="IWritable"/>
    ///   interface. However, the <see cref="IWritable"/> interface cannot be used on value types
    ///   (structs) because of the semantics of calling interface methods that modify the object's
    ///   state on a value type.
    /// </para>
    /// <para>
    ///   To provide serialization support for a value type, you must create a class implementing
    ///   <see cref="IValueWriter{T}"/> for that value type, and mark the value type with the
    ///   <see cref="ValueWriterAttribute"/>. Although you can use <see cref="IValueWriter{T}"/>
    ///   for reference types as well, this is not recommended because <see cref="IValueWriter{T}"/>
    ///   doesn't support record reuse.
    /// </para>
    /// <para>
    ///   Jumbo also provides <see cref="IValueWriter{T}"/> implementations for several built-in
    ///   framework types.
    /// </para>
    /// <para>
    ///   To access the <see cref="IValueWriter{T}"/> implementation for a type, use the
    ///   <see cref="ValueWriter{T}" /> class.
    /// </para>
    /// </remarks>
    public interface IValueWriter<T>
    {
        /// <summary>
        /// Writes the specified value to the specified writer.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="writer">The writer.</param>
        void Write(T value, BinaryWriter writer);

        /// <summary>
        /// Reads a value from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        T Read(BinaryReader reader);
    }
}
