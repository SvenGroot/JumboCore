// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management;
using System.Runtime.Versioning;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides processor usage statistics for all processors in the system.
    /// </summary>
    public sealed class ProcessorStatus : IDisposable
    {
        #region Nested types

        private struct ProcessorStatusData
        {
            public ulong User { get; set; }
            public ulong System { get; set; }
            public ulong Idle { get; set; }
            public ulong IOWait { get; set; }
            public ulong Irq { get; set; }
            public ulong Total { get; set; }
        }

        #endregion

        private readonly ProcessorStatusData[] _processorData;
        private ProcessorStatusData[] _previousProcessorData;
        private StreamReader _procStatReader;
        private readonly List<IndividualProcessorStatus> _processors;
        private ReadOnlyCollection<IndividualProcessorStatus> _processorsReadOnlyWrapper;
        private readonly int _total;
        private readonly char[] _procStatFieldSeparator = new char[] { ' ' };

        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessorStatus"/> class.
        /// </summary>
        public ProcessorStatus()
        {
            _processorData = new ProcessorStatusData[Environment.ProcessorCount + 1];
            Total = new IndividualProcessorStatus(-1);
            _processors = new List<IndividualProcessorStatus>(Environment.ProcessorCount);
            for (int x = 0; x < Environment.ProcessorCount; ++x)
            {
                _processors.Add(new IndividualProcessorStatus(x));
            }
            _processorsReadOnlyWrapper = _processors.AsReadOnly();
            _total = Environment.ProcessorCount;

            Refresh();
        }

        /// <summary>
        /// Gets the usage data for each individual processor in the system.
        /// </summary>
        /// <value>
        /// A collection of <see cref="IndividualProcessorStatus"/> object for each processor.
        /// </value>
        public ReadOnlyCollection<IndividualProcessorStatus> Processors
        {
            get
            {
                return _processorsReadOnlyWrapper;
            }
        }

        /// <summary>
        /// Gets the combined usage data for all processors in the system.
        /// </summary>
        /// <value>
        /// A <see cref="IndividualProcessorStatus"/> object that contains the combined usage data of all processors.
        /// </value>
        public IndividualProcessorStatus Total { get; private set; }

        /// <summary>
        /// Refreshes the usage data.
        /// </summary>
        /// <remarks>
        /// Usage percentages are calculated between two calls of the <see cref="Refresh"/> method. You need to call the <see cref="Refresh"/> method
        /// at least once to get data.
        /// </remarks>
        public void Refresh()
        {
            if (OperatingSystem.IsWindows())
                RefreshWindows();
            else if (OperatingSystem.IsLinux())
                RefreshUnix();

            Recalculate();
        }

        [SupportedOSPlatform("windows")]
        private void RefreshWindows()
        {
            SelectQuery query = new SelectQuery("Win32_PerfRawData_PerfOS_Processor", null, new[] { "Name", "PercentUserTime", "PercentPrivilegedTime", "PercentIdleTime", "PercentInterruptTime", "TimeStamp_Sys100NS" });
            using (ManagementObjectSearcher searcher = new ManagementObjectSearcher(query))
            {

                foreach (ManagementBaseObject obj in searcher.Get())
                {
                    int index;
                    string name = (string)obj.GetPropertyValue("Name");
                    if (name == "_Total")
                    {
                        index = _total;
                    }
                    else
                    {
                        index = Convert.ToInt32(name, CultureInfo.InvariantCulture);
                    }

                    // Despite the names, these properties are *not* percentages for PerfRawData.
                    _processorData[index].User = (ulong)obj.GetPropertyValue("PercentUserTime");
                    _processorData[index].System = (ulong)obj.GetPropertyValue("PercentPrivilegedTime");
                    _processorData[index].Idle = (ulong)obj.GetPropertyValue("PercentIdleTime");
                    _processorData[index].Irq = (ulong)obj.GetPropertyValue("PercentInterruptTime");
                    _processorData[index].Total = (ulong)obj.GetPropertyValue("TimeStamp_Sys100NS");
                }
            }
        }

        private void RefreshUnix()
        {
            if (_procStatReader == null)
                _procStatReader = File.OpenText("/proc/stat");
            else
            {
                _procStatReader.DiscardBufferedData();
                _procStatReader.BaseStream.Position = 0;
            }

            ProcessProcStatLine(_total); // First line is total for all CPUs.
            for (int x = 0; x < Environment.ProcessorCount; ++x)
            {
                ProcessProcStatLine(x);
            }
        }

        private void ProcessProcStatLine(int cpuIndex)
        {
            string line = _procStatReader.ReadLine();
            if (!line.StartsWith("cpu", StringComparison.Ordinal))
                throw new FormatException("Unexpected /proc/stat format.");

            string[] items = line.Split(_procStatFieldSeparator, StringSplitOptions.RemoveEmptyEntries);
            _processorData[cpuIndex].User = Convert.ToUInt64(items[1], CultureInfo.InvariantCulture) + Convert.ToUInt64(items[2], CultureInfo.InvariantCulture); // user + nice
            _processorData[cpuIndex].System = Convert.ToUInt64(items[3], CultureInfo.InvariantCulture); // system
            _processorData[cpuIndex].Idle = Convert.ToUInt64(items[4], CultureInfo.InvariantCulture); // idle
            _processorData[cpuIndex].IOWait = Convert.ToUInt64(items[5], CultureInfo.InvariantCulture); // iowait
            _processorData[cpuIndex].Irq = Convert.ToUInt64(items[6], CultureInfo.InvariantCulture) + Convert.ToUInt64(items[7], CultureInfo.InvariantCulture); // irq + softirq
            _processorData[cpuIndex].Total = _processorData[cpuIndex].User + _processorData[cpuIndex].System + _processorData[cpuIndex].Idle + _processorData[cpuIndex].IOWait + _processorData[cpuIndex].Irq;

            // Some later kernel versions have extra fields for virtualized environments, which we want to include in the total.
            for (int x = 8; x < items.Length; ++x)
            {
                _processorData[cpuIndex].Total += Convert.ToUInt64(items[x], CultureInfo.InvariantCulture);
            }
        }

        private void Recalculate()
        {
            if (_previousProcessorData != null)
            {
                for (int x = 0; x < _processorData.Length; ++x)
                {
                    ulong userDelta = _processorData[x].User - _previousProcessorData[x].User;
                    ulong systemDelta = _processorData[x].System - _previousProcessorData[x].System;
                    ulong idleDelta = _processorData[x].Idle - _previousProcessorData[x].Idle;
                    ulong irqDelta = _processorData[x].Irq - _previousProcessorData[x].Irq;
                    ulong ioWaitDelta = _processorData[x].IOWait - _previousProcessorData[x].IOWait;
                    ulong totalDelta = _processorData[x].Total - _previousProcessorData[x].Total;
                    float factor = 100.0f / totalDelta;

                    IndividualProcessorStatus processor = (x == Environment.ProcessorCount) ? Total : _processors[x];
                    processor.PercentUserTime = factor * userDelta;
                    processor.PercentSystemTime = factor * systemDelta;
                    processor.PercentIdleTime = factor * idleDelta;
                    processor.PercentInterruptTime = factor * irqDelta;
                    processor.PercentIOWaitTime = factor * ioWaitDelta;
                }
            }
            else
            {
                _previousProcessorData = new ProcessorStatusData[Environment.ProcessorCount + 1];
            }

            Array.Copy(_processorData, _previousProcessorData, _processorData.Length);
        }

        #region IDisposable Members

        /// <summary>
        /// Releases all resources associated with the <see cref="ProcessorStatus"/> class.
        /// </summary>
        public void Dispose()
        {
            if (_procStatReader != null)
                _procStatReader.Dispose();
        }

        #endregion
    }
}
