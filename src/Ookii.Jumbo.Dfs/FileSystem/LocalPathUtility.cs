// Copyright (c) Sven Groot (Ookii.org)
using System.IO;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    sealed class LocalPathUtility : IFileSystemPathUtility
    {
        public char DirectorySeparator
        {
            get { return Path.DirectorySeparatorChar; }
        }

        public bool IsPathRooted(string? path)
        {
            return Path.IsPathRooted(path);
        }

        public string Combine(string path1, string path2)
        {
            return Path.Combine(path1, path2);
        }

        public string? GetFileName(string? path)
        {
            return Path.GetFileName(path);
        }

        public string? GetDirectoryName(string? path)
        {
            return Path.GetDirectoryName(path);
        }
    }
}
