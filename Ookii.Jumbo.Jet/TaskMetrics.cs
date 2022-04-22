﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using System.IO;
using System.Xml.Linq;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides information about the read and write operations done by a task.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The <see cref="InputRecords"/>, <see cref="InputBytes"/>, <see cref="OutputRecords"/> and <see cref="OutputBytes"/>
    ///   properties provide information about the amount of data processed and generated by this task. They do not take
    ///   compression or the source or destination of the data into account.
    /// </para>
    /// <para>
    ///   The remaining properties provide information about the amount of I/O activity performed by the task. For
    ///   instance <see cref="LocalBytesRead"/> tells you how much data was read from the local disk. This can include
    ///   data that was first written to the disk by a channel or record reader and then read again. Because of this
    ///   and things like compression, this number doesn't need to match <see cref="InputBytes"/>.
    /// </para>
    /// </remarks>
    [Serializable]
    public sealed class TaskMetrics
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskMetrics));

        /// <summary>
        /// Gets or sets the number of bytes read from the Distributed File System.
        /// </summary>
        /// <value>The number of DFS bytes read.</value>
        public long DfsBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the Distributed File System.
        /// </summary>
        /// <value>The number of DFS bytes written.</value>
        public long DfsBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read from the local disk.
        /// </summary>
        /// <value>The number of local bytes read.</value>
        public long LocalBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written to the local disk.
        /// </summary>
        /// <value>The number of local bytes written.</value>
        public long LocalBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes read over the network by the file and TCP channels.
        /// </summary>
        /// <value>The number of network bytes read.</value>
        public long NetworkBytesRead { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes written over the network by the TCP channel.
        /// </summary>
        /// <value>The number of network bytes written.</value>
        public long NetworkBytesWritten { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes that this task had as input.
        /// </summary>
        /// <value>The number of input bytes.</value>
        public long InputBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of records read.
        /// </summary>
        /// <value>The number of input records.</value>
        public long InputRecords { get; set; }

        /// <summary>
        /// Gets or sets the number of bytes that this task had as output.
        /// </summary>
        /// <value>The number of output bytes.</value>
        public long OutputBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of records written.
        /// </summary>
        /// <value>The number of records written.</value>
        public long OutputRecords { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions that the task received through dynamic partition assignment.
        /// </summary>
        /// <value>The number of dynamically assigned partitions.</value>
        public int DynamicallyAssignedPartitions { get; set; }

        /// <summary>
        /// Gets or sets the number of partitions that were discarded because the had been reassigned to another task.
        /// </summary>
        /// <value>The number of discarded partitions.</value>
        public int DiscardedPartitions { get; set; }

        /// <summary>
        /// Adds the value of the specified <see cref="TaskMetrics"/> instance to this instance.
        /// </summary>
        /// <param name="other">A <see cref="TaskMetrics"/> instance.</param>
        public void Add(TaskMetrics other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            DfsBytesRead += other.DfsBytesRead;
            DfsBytesWritten += other.DfsBytesWritten;
            InputBytes += other.InputBytes;
            InputRecords += other.InputRecords;
            LocalBytesRead += other.LocalBytesRead;
            LocalBytesWritten += other.LocalBytesWritten;
            NetworkBytesRead += other.NetworkBytesRead;
            NetworkBytesWritten += other.NetworkBytesWritten;
            OutputBytes += other.OutputBytes;
            OutputRecords += other.OutputRecords;
            DynamicallyAssignedPartitions += other.DynamicallyAssignedPartitions;
            DiscardedPartitions += other.DynamicallyAssignedPartitions;
        }

        /// <summary>
        /// Returns a string representation of the <see cref="TaskMetrics"/> object.
        /// </summary>
        /// <returns>A string representation of the <see cref="TaskMetrics"/> object.</returns>
        public override string ToString()
        {
            using (var result = new StringWriter(CultureInfo.CurrentCulture))
            {
                result.WriteLine("Input records: {0}", InputRecords);
                result.WriteLine("Output records: {0}", OutputRecords);
                result.WriteLine("Input bytes: {0}", InputBytes);
                result.WriteLine("Output bytes: {0}", OutputBytes);
                result.WriteLine("DFS bytes read: {0}", DfsBytesRead);
                result.WriteLine("DFS bytes written: {0}", DfsBytesWritten);
                result.WriteLine("Local bytes read: {0}", LocalBytesRead);
                result.WriteLine("Local bytes written: {0}", LocalBytesWritten);
                result.WriteLine("Channel network bytes read: {0}", NetworkBytesRead);
                result.WriteLine("Channel network bytes written: {0}", NetworkBytesWritten);
                result.WriteLine("Additional partitions: {0}", DynamicallyAssignedPartitions);
                result.WriteLine("Discarded partitions: {0}", DiscardedPartitions);
                return result.ToString();
            }
        }

        /// <summary>
        /// Writes the metrics to the log.
        /// </summary>
        public void LogMetrics()
        {
            _log.InfoFormat("Input records: {0}", InputRecords);
            _log.InfoFormat("Output records: {0}", OutputRecords);
            _log.InfoFormat("Input bytes: {0}", InputBytes);
            _log.InfoFormat("Output bytes: {0}", OutputBytes);
            _log.InfoFormat("DFS bytes read: {0}", DfsBytesRead);
            _log.InfoFormat("DFS bytes written: {0}", DfsBytesWritten);
            _log.InfoFormat("Local bytes read: {0}", LocalBytesRead);
            _log.InfoFormat("Local bytes written: {0}", LocalBytesWritten);
            _log.InfoFormat("Channel network bytes read: {0}", NetworkBytesRead);
            _log.InfoFormat("Channel network bytes written: {0}", NetworkBytesWritten);
            _log.InfoFormat("Additional partitions: {0}", DynamicallyAssignedPartitions);
            _log.InfoFormat("Discarded partitions: {0}", DiscardedPartitions);
        }

        /// <summary>
        /// Returns the value of this <see cref="TaskMetrics"/> object as an <see cref="XElement"/>.
        /// </summary>
        /// <returns>The value of this <see cref="TaskMetrics"/> object as an <see cref="XElement"/>.</returns>
        public XElement ToXml()
        {
            return new XElement("Metrics",
                new XElement("InputRecords", InputRecords),
                new XElement("OutputRecords", OutputRecords),
                new XElement("InputBytes", InputBytes),
                new XElement("OutputBytes", OutputBytes),
                new XElement("DfsBytesRead", DfsBytesRead),
                new XElement("DfsBytesWritten", DfsBytesWritten),
                new XElement("LocalBytesRead", LocalBytesRead),
                new XElement("LocalBytesWritten", LocalBytesWritten),
                new XElement("NetworkBytesRead", NetworkBytesRead),
                new XElement("NetworkBytesWritten", NetworkBytesWritten),
                new XElement("DynamicallyAssignedPartitions", DynamicallyAssignedPartitions),
                new XElement("DiscardedPartitions", DiscardedPartitions)
                );
        }

        /// <summary>
        /// Creates a <see cref="TaskMetrics"/> instance from an XML element.
        /// </summary>
        /// <param name="element">The element. May be <see langword="null"/>.</param>
        /// <returns>A <see cref="TaskMetrics"/> object created from the XML element, or <see langword="null"/> if <paramref name="element"/> was <see langword="null"/>.</returns>
        public static TaskMetrics FromXml(XElement element)
        {
            if (element == null)
                return null;
            if (element.Name != "Metrics")
                throw new ArgumentException("Invalid metrics element.", nameof(element));

            return new TaskMetrics()
            {
                InputRecords = (long)element.Element("InputRecords"),
                OutputRecords = (long)element.Element("OutputRecords"),
                InputBytes = (long)element.Element("InputBytes"),
                OutputBytes = (long)element.Element("OutputBytes"),
                DfsBytesRead = (long)element.Element("DfsBytesRead"),
                DfsBytesWritten = (long)element.Element("DfsBytesWritten"),
                LocalBytesRead = (long)element.Element("LocalBytesRead"),
                LocalBytesWritten = (long)element.Element("LocalBytesWritten"),
                NetworkBytesRead = (long)element.Element("NetworkBytesRead"),
                NetworkBytesWritten = (long)element.Element("NetworkBytesWritten"),
                DynamicallyAssignedPartitions = (int)element.Element("DynamicallyAssignedPartitions"),
                DiscardedPartitions = (int)element.Element("DiscardedPartitions")
            };
        }
    }
}
