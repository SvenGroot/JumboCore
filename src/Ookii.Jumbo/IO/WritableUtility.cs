// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.Serialization;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Provides utility functions for creating <see cref="IWritable"/> implementations.
    /// </summary>
    public static class WritableUtility
    {
        /// <summary>
        /// Gets an uninitialized object of a type implementing <see cref="IWritable"/>.
        /// </summary>
        /// <typeparam name="T">The type of the object to create.</typeparam>
        /// <returns>An uninitialized instance of <typeparamref name="T"/>.</returns>
        /// <remarks>
        /// <para>
        ///   The constructor of <typeparamref name="T"/> will not have been invoked.
        /// </para>
        /// </remarks>
        public static T GetUninitializedWritable<T>()
            where T : class, IWritable
            => (T)FormatterServices.GetUninitializedObject(typeof(T));

        /// <summary>
        /// Writes a 32-bit integer in a compressed format.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to write the value to.</param>
        /// <param name="value">The 32-bit integer to be written.</param>
        public static void Write7BitEncodedInt32(BinaryWriter writer, int value)
        {
            ArgumentNullException.ThrowIfNull(writer);
            var uintValue = (uint)value; // this helps support negative numbers, not really needed but anyway.
            while (uintValue >= 0x80)
            {
                writer.Write((byte)(uintValue | 0x80));
                uintValue = uintValue >> 7;
            }
            writer.Write((byte)uintValue);
        }

        /// <summary>
        /// Reads in a 32-bit integer in compressed format.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to read the value from.</param>
        /// <returns>A 32-bit integer in compressed format. </returns>
        public static int Read7BitEncodedInt32(BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);
            byte currentByte;
            var result = 0;
            var bits = 0;
            do
            {
                if (bits == 35)
                {
                    throw new FormatException("Invalid 7-bit encoded int.");
                }
                currentByte = reader.ReadByte();
                result |= (currentByte & 0x7f) << bits;
                bits += 7;
            }
            while ((currentByte & 0x80) != 0);
            return result;
        }
    }
}
