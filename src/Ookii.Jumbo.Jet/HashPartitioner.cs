// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// A partitioner based on the value returned by <see cref="IEqualityComparer{T}.GetHashCode(T)"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   You can customize the behaviour of the <see cref="HashPartitioner{T}"/> by specifying a custom <see cref="IEqualityComparer{T}"/>.
    ///   To do this, specify the type name of the custom comparer in the <see cref="Jobs.StageConfiguration.StageSettings"/> of the stage
    ///   that produces the records to be partitioned using the <see cref="PartitionerConstants.EqualityComparerSetting"/> key.
    /// </para>
    /// <para>
    ///   If you don't specify a comparer, <see cref="EqualityComparer{T}.Default"/> will be used.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The type of the values to partition.</typeparam>
    public class HashPartitioner<T> : Configurable, IPartitioner<T>
        where T : notnull
    {
        private IEqualityComparer<T>? _comparer = EqualityComparer<T>.Default;

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            _comparer = null;
            if (TaskContext != null)
            {
                var comparerTypeName = TaskContext.StageConfiguration.GetSetting(PartitionerConstants.EqualityComparerSetting, null);
                if (!string.IsNullOrEmpty(comparerTypeName))
                    _comparer = (IEqualityComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true)!, DfsConfiguration, JetConfiguration, TaskContext);
            }

            if (_comparer == null)
                _comparer = EqualityComparer<T>.Default;
        }

        #region IPartitioner<T> Members

        /// <summary>
        /// Gets or sets the number of partitions.
        /// </summary>
        public int Partitions { get; set; }

        /// <summary>
        /// Gets the partition for the specified value.
        /// </summary>
        /// <param name="value">The value to be partitioned.</param>
        /// <returns>The partition number for the specified value.</returns>
        public int GetPartition(T value)
        {
            // IEqualityComparer<T>.GetHashCode should return 0 when value == null.
            return (_comparer!.GetHashCode(value) & int.MaxValue) % Partitions;
        }

        #endregion
    }
}
