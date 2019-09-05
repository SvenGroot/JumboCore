// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    sealed class DfsPathUtility : IFileSystemPathUtility
    {
        public char DirectorySeparator
        {
            get { return DfsPath.DirectorySeparator; }
        }

        public bool IsPathRooted(string path)
        {
            return DfsPath.IsPathRooted(path);
        }

        public string Combine(string path1, string path2)
        {
            return DfsPath.Combine(path1, path2);
        }

        public string GetFileName(string path)
        {
            return DfsPath.GetFileName(path);
        }

        public string GetDirectoryName(string path)
        {
            return DfsPath.GetDirectoryName(path);
        }
    }
}
