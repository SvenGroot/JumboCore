// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo;
using System.Globalization;

/// <summary>
/// Summary description for FileSystemEntryInfo
/// </summary>
public class FileSystemEntryInfo
{
    public FileSystemEntryInfo()
    {
    }

    public FileSystemEntryInfo(JumboFileSystemEntry entry, bool includeChildren)
    {
        Name = entry.Name;
        DateCreated = entry.DateCreated.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        FullPath = entry.FullPath;
        JumboFile file = entry as JumboFile;
        if( file != null )
        {
            SizeInBytes = file.Size.ToString("#,##0", CultureInfo.InvariantCulture);
            FormattedSize = new BinarySize(file.Size).ToString("0.##SB", CultureInfo.InvariantCulture);
            BlockSize = new BinarySize(file.BlockSize).ToString("AB", CultureInfo.InvariantCulture);
            ReplicationFactor = file.ReplicationFactor;
            BlockCount = file.Blocks.Count;
            RecordOptions = file.RecordOptions.ToString();
        }
        else
        {
            JumboDirectory dir = (JumboDirectory)entry;
            IsDirectory = true;
            if( includeChildren )
            {
                Children = (from child in dir.Children
                            orderby !(child is JumboDirectory), child.Name
                            select new FileSystemEntryInfo(child, false)).ToArray();
            }
        }
    }

    public string Name { get; set; }

    public string FullPath { get; set; }

    public bool IsDirectory { get; set; }

    public string SizeInBytes { get; set; }

    public string FormattedSize { get; set; }

    public string DateCreated { get; set; }

    public FileSystemEntryInfo[] Children { get; set; }

    public string BlockSize { get; set; }

    public int ReplicationFactor { get; set; }

    public int BlockCount { get; set; }

    public string RecordOptions { get; set; }
}
