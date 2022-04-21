// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs;

namespace NameServerApplication
{
    class PendingFile
    {
        public PendingFile(DfsFile file)
        {
            File = file;
        }

        public DfsFile File { get; private set; }
        public Guid? PendingBlock { get; set; }

        public void SaveToFileSystemImage(BinaryWriter writer)
        {
            writer.Write(File.FullPath);
            writer.Write(PendingBlock.HasValue);
            if (PendingBlock != null)
                writer.Write(PendingBlock.Value.ToByteArray());
        }
    }
}
