// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Management;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Ookii.Jumbo;

/// <summary>
/// Represents a snapshot of the system's memory status.
/// </summary>
public sealed class MemoryStatus : IDisposable
{
    private long _totalPhysicalMemory;
    private long _availablePhysicalMemory;
    private long _cachedMemory;
    private long _bufferedMemory;
    private long _totalSwap;
    private long _availableSwap;
    private StreamReader? _procMemInfoReader;

    /// <summary>
    /// Initializes a new instance of the <see cref="MemoryStatus"/> class.
    /// </summary>
    public MemoryStatus()
    {
        Refresh();
    }

    /// <summary>
    /// Gets the total size of the physical memory.
    /// </summary>
    /// <value>
    /// The total physical memory, in bytes.
    /// </value>
    public long TotalPhysicalMemory
    {
        get { return _totalPhysicalMemory; }
    }

    /// <summary>
    /// Gets the amount of available physical memory.
    /// </summary>
    /// <value>
    /// The available physical memory, in bytes.
    /// </value>
    /// <remarks>
    /// This is not the same as free physical memory. Cached memory is considered available.
    /// </remarks>
    public long AvailablePhysicalMemory
    {
        get { return _availablePhysicalMemory; }
    }

    /// <summary>
    /// Gets the amount of memory that is used as cache. This value is not available on Windows.
    /// </summary>
    /// <value>
    /// The amount of memory that is used as cache, in bytes.
    /// </value>
    public long CachedMemory
    {
        get { return _cachedMemory; }
    }

    /// <summary>
    /// Gets the amount of memory that is used as buffers. This value is not available on Windows.
    /// </summary>
    /// <value>
    /// The amount of memory that is used as buffers, in bytes.
    /// </value>
    public long BufferedMemory
    {
        get { return _bufferedMemory; }
    }

    /// <summary>
    /// Gets the total size of the swap space.
    /// </summary>
    /// <value>
    /// The total size of the swap space, bytes.
    /// </value>
    public long TotalSwap
    {
        get { return _totalSwap; }
    }

    /// <summary>
    /// Gets the amount of swap space available.
    /// </summary>
    /// <value>
    /// The available swap space, in bytes.
    /// </value>
    public long AvailableSwap
    {
        get { return _availableSwap; }
    }

    /// <summary>
    /// Refreshes the memory snapshot.
    /// </summary>
    public void Refresh()
    {
        if (OperatingSystem.IsWindows())
        {
            RefreshWindows();
        }
        else if (OperatingSystem.IsLinux())
        {
            RefreshUnix();
        }
    }

    /// <summary>
    /// Returns a string representation of the current <see cref="MemoryStatus"/>.
    /// </summary>
    /// <returns>A string representation of the current <see cref="MemoryStatus"/>.</returns>
    public override string ToString()
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "Physical: {0}M total, {1}M available; Page file: {2}M total, {3}M available.", TotalPhysicalMemory / BinarySize.Mebi, AvailablePhysicalMemory / BinarySize.Mebi, TotalSwap / BinarySize.Mebi, AvailableSwap / BinarySize.Mebi);
        }
        else if (Environment.OSVersion.Platform == PlatformID.Unix)
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "Physical: {0}M total, {1}M available, {2}M buffered, {3}M cached; Swap: {4}M total, {5}M available.", TotalPhysicalMemory / BinarySize.Mebi, AvailablePhysicalMemory / BinarySize.Mebi, BufferedMemory / BinarySize.Mebi, CachedMemory / BinarySize.Mebi, TotalSwap / BinarySize.Mebi, AvailableSwap / BinarySize.Mebi);
        }
        else
        {
            return "No memory information.";
        }
    }

    [SupportedOSPlatform("windows")]
    private void RefreshWindows()
    {
        if (!NativeMethods.GetPerformanceInfo(out var performanceInfo, Marshal.SizeOf(typeof(NativeMethods.PERFORMANCE_INFORMATION))))
        {
            throw new System.ComponentModel.Win32Exception();
        }

        _totalPhysicalMemory = (long)performanceInfo.PhysicalTotal * (long)performanceInfo.PageSize;
        _availablePhysicalMemory = (long)performanceInfo.PhysicalAvailable * (long)performanceInfo.PageSize;

        var query = new SelectQuery("Win32_PageFileUsage", "", new[] { "CurrentUsage", "AllocatedBaseSize" });
        using (var searcher = new ManagementObjectSearcher(query))
        {
            _totalSwap = 0;
            _availableSwap = 0;
            foreach (var obj in searcher.Get())
            {
                var size = (uint)obj["AllocatedBaseSize"] * BinarySize.Mebi;
                var used = (uint)obj["CurrentUsage"] * BinarySize.Mebi;
                _totalSwap += size;
                _availableSwap += (size - used);
            }
        }
    }

    private void RefreshUnix()
    {
        if (_procMemInfoReader == null)
        {
            if (File.Exists("/proc/meminfo"))
            {
                _procMemInfoReader = File.OpenText("/proc/meminfo");
            }
            else
            {
                return;
            }
        }
        else
        {
            _procMemInfoReader.DiscardBufferedData();
            _procMemInfoReader.BaseStream.Position = 0;
        }

        var neededFields = 6;
        string? line;
        while (neededFields > 0 && (line = _procMemInfoReader.ReadLine()) != null)
        {
            if (ExtractMemInfoValue(line, "MemTotal:", ref _totalPhysicalMemory) ||
                ExtractMemInfoValue(line, "MemFree:", ref _availablePhysicalMemory) ||
                ExtractMemInfoValue(line, "Buffers:", ref _bufferedMemory) ||
                ExtractMemInfoValue(line, "Cached:", ref _cachedMemory) ||
                ExtractMemInfoValue(line, "SwapTotal:", ref _totalSwap) ||
                ExtractMemInfoValue(line, "SwapFree:", ref _availableSwap))
            {
                --neededFields;
            }
        }

        // Correct for the difference between free and available.
        _availablePhysicalMemory += _cachedMemory;
    }

    private static bool ExtractMemInfoValue(string line, string field, ref long value)
    {
        if (line.StartsWith(field, StringComparison.Ordinal))
        {
            // Strip the field, the colon, and the kB
            var valueString = line.Substring(field.Length + 1, line.Length - field.Length - 3);
            value = long.Parse(valueString, System.Globalization.CultureInfo.InvariantCulture) * BinarySize.Kibi;
            return true;
        }
        else
        {
            return false;
        }
    }

    #region IDisposable Members

    /// <summary>
    /// Releases all resources used by the <see cref="MemoryStatus"/> class.
    /// </summary>
    public void Dispose()
    {

        if (_procMemInfoReader != null)
        {
            _procMemInfoReader.Dispose();
        }
    }

    #endregion
}
