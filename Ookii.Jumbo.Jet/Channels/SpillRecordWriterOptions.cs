using System;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Flags controlling the behavior of the <see cref="SpillRecordWriter{T}"/>.
    /// </summary>
    [Flags]
    public enum SpillRecordWriterOptions
    {
        /// <summary>
        /// Default behavior is used. Records will never wrap around the end of the buffer, and index entries always describe only one record.
        /// </summary>
        None = 0,
        /// <summary>
        /// Records are allowed to wrap around the end of the circular buffer.
        /// </summary>
        AllowRecordWrapping = 1,
        /// <summary>
        /// Index entries for multiple records of the same partition are allowed to be merged.
        /// </summary>
        AllowMultiRecordIndexEntries = 1 << 1,
    }
}
