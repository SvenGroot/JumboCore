// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Performs operations on strings that contain file or directory path information for the distributed file system.
    /// </summary>
    public static class DfsPath
    {
        /// <summary>
        /// The character that separates directory names in a path.
        /// </summary>
        public const char DirectorySeparator = '/';

        /// <summary>
        /// Determines if the specified path is rooted.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns><see langword="true"/> if the path is rooted; otherwise, <see langword="false"/>.</returns>
        public static bool IsPathRooted(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));
            return path.Length > 0 && path[0] == DirectorySeparator;
        }

        /// <summary>
        /// Combines two paths.
        /// </summary>
        /// <param name="path1">The first path.</param>
        /// <param name="path2">The second path.</param>
        /// <returns>The combined path.</returns>
        public static string Combine(string path1, string path2)
        {
            if (path1 == null)
                throw new ArgumentNullException(nameof(path1));
            if (path2 == null)
                throw new ArgumentNullException(nameof(path2));

            if (path2.Length == 0)
                return path1;
            if (path1.Length == 0)
                return path2;

            if (IsPathRooted(path2))
                return path2;

            string result = path1;
            if (path1[path1.Length - 1] != DirectorySeparator)
                result += DirectorySeparator;
            result += path2;
            return result;
        }

        /// <summary>
        /// Returns the file name and extension of the specified path string.
        /// </summary>
        /// <param name="path">The path string from which to obtain the file name and extension.</param>
        /// <returns>The file name and extension of the specified path string.</returns>
        public static string GetFileName(string path)
        {
            if (path != null)
            {
                int length = path.Length;
                int current = length;
                while (--current >= 0)
                {
                    char ch = path[current];
                    if (ch == DirectorySeparator)
                    {
                        return path.Substring(current + 1, (length - current) - 1);
                    }
                }
            }
            return path;
        }

        /// <summary>
        /// Returns the directory information for the specified path string.
        /// </summary>
        /// <param name="path">The path of a file or directory.</param>
        /// <returns>Directory information for <paramref name="path"/>, or <see langword="null"/> if <paramref name="path"/> denotes a root directory. return <see cref="String.Empty"/> if <paramref name="path"/> does
        /// not contain directory information.</returns>
        public static string GetDirectoryName(string path)
        {
            if (path != null)
            {
                if (path == "/")
                    return null;
                int index = path.LastIndexOf(DirectorySeparator);
                if (index == 0)
                    return "/";
                else if (index > 0)
                    return path.Substring(0, index);
                else
                    return "";
            }
            return null;
        }
    }
}
