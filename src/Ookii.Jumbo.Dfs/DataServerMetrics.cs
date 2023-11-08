// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Provides metrics about a data server.
/// </summary>
[GeneratedWritable]
public partial class DataServerMetrics : ServerMetrics
{
    /// <summary>
    /// Gets or sets the number of blocks stored on this server.
    /// </summary>
    /// <value>
    /// The number of blocks stored on this server.
    /// </value>
    public int BlockCount { get; set; }

    /// <summary>
    /// Gets or sets the amount of disk space used by the block files.
    /// </summary>
    /// <value>
    /// The amount of disk space used by the block files, in bytes.
    /// </value>
    public long DiskSpaceUsed { get; set; }

    /// <summary>
    /// Gets or sets the amount of free disk space on the disk holding the blocks.
    /// </summary>
    /// <value>
    /// The amount of free disk space on the disk holding the blocks, in bytes.
    /// </value>
    public long DiskSpaceFree { get; set; }

    /// <summary>
    /// Gets or sets the total size of the disk holding the blocks.
    /// </summary>
    /// <value>
    /// The total size of the disk holding the blocks, in bytes.
    /// </value>
    public long DiskSpaceTotal { get; set; }

    /// <summary>
    /// Gets a string representation of the <see cref="DataServerMetrics"/>.
    /// </summary>
    /// <returns>A string representation of the <see cref="DataServerMetrics"/>.</returns>
    public override string ToString()
    {
        return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0}; {1} blocks; Used: {2:#,0}B; Free: {3:#,0}B; Total: {4:#,0}B", base.ToString(), BlockCount, DiskSpaceUsed, DiskSpaceFree, DiskSpaceTotal);
    }
}
