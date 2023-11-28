// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Delegate used by <see cref="FileSystem.DfsClient"/> to report progress on various operations.
/// </summary>
/// <param name="fileName">The DFS file name to which the progress applies.</param>
/// <param name="progressPercentage">The progress percentage, between 0 and 100.</param>
/// <param name="progressBytes">The amount of bytes processed so far.</param>
public delegate void ProgressCallback(string fileName, int progressPercentage, long progressBytes);
