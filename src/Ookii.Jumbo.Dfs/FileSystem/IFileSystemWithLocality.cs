// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;

namespace Ookii.Jumbo.Dfs.FileSystem;

/// <summary>
/// Represents a file system that incorporates knowledge about the locations of file splits.
/// </summary>
public interface IFileSystemWithLocality
{
    /// <summary>
    /// Gets the locations where the part of the file beginning with the specified offset is stored.
    /// </summary>
    /// <param name="file">The file.</param>
    /// <param name="offset">The offset.</param>
    /// <returns>The host names of the location.</returns>
    IEnumerable<string> GetLocationsForOffset(JumboFile file, long offset);
}
