// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Value writer for <see cref="UInt128"/>.
    /// </summary>
    public class UInt128Writer : IValueWriter<UInt128>
    {
        /// <summary>
        /// Writes the specified value to the specified writer.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="writer">The writer.</param>
        public void Write(UInt128 value, System.IO.BinaryWriter writer)
        {
            writer.Write(value.High64);
            writer.Write(value.Low64);
        }

        /// <summary>
        /// Reads a value from the specified reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public UInt128 Read(System.IO.BinaryReader reader)
        {
            ulong high64 = reader.ReadUInt64();
            ulong low64 = reader.ReadUInt64();
            return new UInt128(high64, low64);
        }
    }
}
