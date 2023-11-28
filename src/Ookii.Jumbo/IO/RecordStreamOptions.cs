// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Indicates how a record stream should treat records.
/// </summary>
[Flags]
public enum RecordStreamOptions
{
    /// <summary>
    /// No special handlings of records is required.
    /// </summary>
    None,
    /// <summary>
    /// Records should not cross boundaries (e.g. block boundaries on the DFS).
    /// </summary>
    DoNotCrossBoundary
}
