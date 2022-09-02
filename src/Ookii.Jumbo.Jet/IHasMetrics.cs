// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides additional metrics about disk and network activity.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Can be used by channels and record readers and writers to provide additional
    ///   metrics. If you use this interface on a record reader or writer it should only
    ///   consider reads/writes that are not already reported via the regular <see cref="Ookii.Jumbo.IO.RecordReader{T}.InputBytes"/>
    ///   and <see cref="Ookii.Jumbo.IO.RecordWriter{T}.OutputBytes"/> properties.
    /// </para>
    /// <para>
    ///   It is used by e.g. the <see cref="MergeRecordReader{T}"/> to report the additional reads and writes
    ///   it does if more than one merge pass is used.
    /// </para>
    /// </remarks>
    public interface IHasMetrics
    {
        /// <summary>
        /// Gets the number of bytes read from the local disk.
        /// </summary>
        /// <value>The local bytes read.</value>
        long LocalBytesRead { get; }

        /// <summary>
        /// Gets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The local bytes written.</value>
        long LocalBytesWritten { get; }

        /// <summary>
        /// Gets the number of bytes read over the network.
        /// </summary>
        /// <value>The network bytes read.</value>
        /// <remarks>
        /// <para>
        ///   Only channels should normally use this property.
        /// </para>
        /// </remarks>
        long NetworkBytesRead { get; }

        /// <summary>
        /// Gets the number of bytes written over the network.
        /// </summary>
        /// <value>The network bytes written.</value>
        /// <remarks>
        /// <para>
        ///   Only channels should normally use this property.
        /// </para>
        /// </remarks>
        long NetworkBytesWritten { get; }
    }
}
