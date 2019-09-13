// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Attribute for task classes that indicates that all input partitions should be processed by the same task instance.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The only time a task can have multiple input partitions is if its input is a file channel and <see cref="Channels.ChannelConfiguration.PartitionsPerTask"/>
    ///   is larger than 1. This attribute has no effect on child tasks of a compound task with internal partitioning.
    /// </para>
    /// <para>
    ///   If the task that this attribute is applied to is a pull task, it can determine what partition is being processed
    ///   and how many partitions there are by casting the input record reader to a <see cref="MultiPartitionRecordReader{T}"/>.
    /// </para>
    /// <para>
    ///   However, if the input to a pull task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute is not
    ///   a channel with multiple partitions per task, the input record reader will not be a <see cref="MultiPartitionRecordReader{T}"/>
    ///   so you should not assume that such a cast will always succeed.
    /// </para>
    /// <para>
    ///   The input partition affinity of the output of a task with the <see cref="ProcessAllInputPartitionsAttribute"/> attribute
    ///   is lost. If the output is a child task, it cannot determine what the current input partition is, and if the output
    ///   is written to the DFS, it will be a single file (rather than a file for each partition, which is the default).
    /// </para>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple=false, Inherited=true)]
    public sealed class ProcessAllInputPartitionsAttribute : Attribute
    {
    }
}
