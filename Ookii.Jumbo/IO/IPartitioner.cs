// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for classes that can partition a range of values.
    /// </summary>
    /// <typeparam name="T">The type of object to partition.</typeparam>
    public interface IPartitioner<T>
    {
        /// <summary>
        /// Gets or sets the number of partitions.
        /// </summary>
        int Partitions { get; set; }

        /// <summary>
        /// Gets the partition for the specified value.
        /// </summary>
        /// <param name="value">The value to be partitioned.</param>
        /// <returns>The partition number for the specified value.</returns>
        int GetPartition(T value);
    }
}
