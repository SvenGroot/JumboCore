// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace NameServerApplication;

class FileDeletedEventArgs : EventArgs
{
    public FileDeletedEventArgs(DfsFile file, Guid? pendingBlock)
    {
        File = file;
        PendingBlock = pendingBlock;
    }

    public DfsFile File { get; private set; }
    public Guid? PendingBlock { get; private set; }
}
