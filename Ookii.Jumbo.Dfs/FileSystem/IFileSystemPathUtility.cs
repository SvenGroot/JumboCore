// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides helper methods for manipulating paths.
    /// </summary>
    public interface IFileSystemPathUtility
    {
        /// <summary>
        /// Gets the character that separates directory names in a path.
        /// </summary>
        char DirectorySeparator { get; }

        /// <summary>
        /// Determines if the specified path is rooted.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns><see langword="true"/> if the path is rooted; otherwise, <see langword="false"/>.</returns>
        bool IsPathRooted(string path);

        /// <summary>
        /// Combines two paths.
        /// </summary>
        /// <param name="path1">The first path.</param>
        /// <param name="path2">The second path.</param>
        /// <returns>The combined path.</returns>
        string Combine(string path1, string path2);

        /// <summary>
        /// Returns the file name and extension of the specified path string.
        /// </summary>
        /// <param name="path">The path string from which to obtain the file name and extension.</param>
        /// <returns>The characters after the last directory character in path.</returns>
        string GetFileName(string path);

        /// <summary>
        /// Returns the directory information for the specified path string.
        /// </summary>
        /// <param name="path">The path of a file or directory.</param>
        /// <returns>Directory information for <paramref name="path"/>, or <see langword="null"/> if <paramref name="path"/> denotes a root directory. return <see cref="String.Empty"/> if <paramref name="path"/> does
        /// not contain directory information.</returns>
        string GetDirectoryName(string path);
    }
}
