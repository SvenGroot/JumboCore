// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;

namespace NameServerApplication;

class BlockInfo
{
    public BlockInfo(Guid blockId, DfsFile file)
    {
        BlockId = blockId;
        File = file;
        DataServers = new List<DataServerInfo>();
    }

    public Guid BlockId { get; private set; }
    public List<DataServerInfo> DataServers { get; private set; }
    public DfsFile File { get; private set; }
}
