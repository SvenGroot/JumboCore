// Copyright (c) Sven Groot (Ookii.org)
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for objects that can be serialized using the <see cref="BinaryWriter"/>
    /// and <see cref="BinaryReader"/> classes.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The <see cref="IWritable"/> interface provides a simple, light-weight serialization protocol that
    ///   is used primarily by the <see cref="BinaryRecordReader{T}"/> and <see cref="BinaryRecordWriter{T}"/>
    ///   class (but is also used in a few other instances).
    /// </para>
    /// <para>
    ///   Unlike the serialization provided by the <see cref="System.Runtime.Serialization.Formatters.Binary.BinaryFormatter"/>
    ///   class, <see cref="IWritable"/> serialization is done entirely by the class that implements the
    ///   interface. There are no built-in checks to see if the type being deserialized from a stream
    ///   matches the type that was serialized to it. It is up to the implementation of the <see cref="Write"/>
    ///   and <see cref="Read"/> methods to guarantee binary compatibility.
    /// </para>
    /// <para>
    ///   For this reason, it is recommended to use <see cref="IWritable"/> serialization only for
    ///   short-time storage where the performance of serializing and deserializing is important.
    ///   Its primary usage in Jumbo (intermediate data for Jumbo Jet jobs) fits these criteria.
    /// </para>
    /// <para>
    ///   Types that implement <see cref="IWritable"/> must be reference types (classes). To
    ///   support serialization of value types, you must implement a <see cref="IValueWriter{T}"/>
    ///   for those types and mark them with the <see cref="ValueWriterAttribute"/>.
    /// </para>
    /// <para>
    ///   The <see cref="BinaryRecordReader{T}"/> creates new record instances by using the <see cref="System.Runtime.Serialization.FormatterServices.GetUninitializedObject"/>
    ///   method. This means the object's constructors are not invoked, so the <see cref="Read"/> implementation may not make any
    ///   assumptions about the state of the class, and must guarantee the object is in a consistent state after
    ///   it finishes.
    /// </para>
    /// <para>
    ///   When record reuse is enabled, the <see cref="Read"/> method will be called multiple times on
    ///   the same instance. The <see cref="Read"/> method must replace the entire internal state
    ///   of the object with that of the new record. If any of the object's internal state is stored
    ///   in reference types, it's recommended to reuse object instances as much as possible.
    /// </para>
    /// </remarks>
    public interface IWritable
    {
        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        void Write(BinaryWriter writer);

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        /// <remarks>
        /// <para>
        ///   This object instance may have been created using <see cref="System.Runtime.Serialization.FormatterServices.GetUninitializedObject"/>
        ///   so the <see cref="Read"/> method may not make any assumptions about the internal state of the object, and must
        ///   ensure the object is in a consistent state afterwards.
        /// </para>
        /// <para>
        ///   When record reuse is enabled, the <see cref="Read"/> method will be called multiple times on
        ///   the same instance. The <see cref="Read"/> method must replace the entire internal state
        ///   of the object with that of the new record. If any of the object's internal state is stored
        ///   in reference types, it's recommended to reuse object instances as much as possible.
        /// </para>
        /// </remarks>
        void Read(BinaryReader reader);
    }
}
