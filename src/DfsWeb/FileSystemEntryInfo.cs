// $Id$
//
using System;
using System.Globalization;
using System.Linq;
using Ookii;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs.FileSystem;

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
        ArgumentNullException.ThrowIfNull(entry);

        Name = entry.Name;
        DateCreated = entry.DateCreated.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        FullPath = entry.FullPath;
        var file = entry as JumboFile;
        if (file != null)
        {
            SizeInBytes = file.Size.ToString("#,##0", CultureInfo.InvariantCulture);
            FormattedSize = new BinarySize(file.Size).ToString("0.##SB", CultureInfo.InvariantCulture);
            BlockSize = new BinarySize(file.BlockSize).ToString("AB", CultureInfo.InvariantCulture);
            ReplicationFactor = file.ReplicationFactor;
            BlockCount = file.Blocks.Length;
            RecordOptions = file.RecordOptions.ToString();
        }
        else
        {
            var dir = (JumboDirectory)entry;
            IsDirectory = true;
            if (includeChildren)
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
