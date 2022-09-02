// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for writable types that provides a default implementation
    /// of <see cref="IWritable"/> based on reflection and code generation.
    /// </summary>
    /// <typeparam name="T">The type of the writable type; this should be the type inheriting from <see cref="Writable{T}"/>.</typeparam>
    /// <remarks>
    /// <para>
    ///   The generated <see cref="IWritable"/> implementation will serialize all properties of the type. Because <see cref="IWritable"/> types
    ///   may be created uninitialized (without calling the constructor), you must be sure it's safe to deserialize a type by simply setting
    ///   all its properties, even if the constructor hasn't been run.
    /// </para>
    /// <para>
    ///   Because the serializer and deserializer are generated from the type <typeparamref name="T"/>, it is not safe to derive other classes from
    ///   that type unless you override the <see cref="IWritable.Read"/> and <see cref="IWritable.Write"/> implementations.
    /// </para>
    /// </remarks>
    public abstract class Writable<T> : IWritable
        where T : Writable<T>
    {
        private static readonly Action<T, BinaryWriter> _writeMethod = WritableUtility.CreateSerializer<T>();
        private static readonly Action<T, BinaryReader> _readMethod = WritableUtility.CreateDeserializer<T>();

        #region IWritable Members

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public virtual void Write(BinaryWriter writer)
        {
            _writeMethod((T)this, writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            _readMethod((T)this, reader);
        }

        #endregion
    }
}
