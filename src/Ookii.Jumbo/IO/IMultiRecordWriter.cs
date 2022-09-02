// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for record writers that use partitioning.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Multi")]
    public interface IMultiRecordWriter<T>
    {
        /// <summary>
        /// Gets the partitioner.
        /// </summary>
        /// <value>The partitioner.</value>
        IPartitioner<T> Partitioner { get; }
    }
}
