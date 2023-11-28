// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.IO;

/// <summary>
/// Interface for record writers that use partitioning.
/// </summary>
/// <typeparam name="T">The type of the records.</typeparam>
public interface IMultiRecordWriter<T>
    where T : notnull
{
    /// <summary>
    /// Gets the partitioner.
    /// </summary>
    /// <value>The partitioner.</value>
    IPartitioner<T> Partitioner { get; }
}
