// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Topology;

namespace NameServerApplication;

class DataServerInfo : TopologyNode
{
    private readonly List<HeartbeatResponse> _pendingResponses = new List<HeartbeatResponse>();
    private readonly HashSet<Guid> _blocks = new HashSet<Guid>();
    private readonly HashSet<Guid> _pendingBlocks = new HashSet<Guid>();
    private readonly Guid _fileSystemId;

    public DataServerInfo(ServerAddress address, Guid fileSystemId)
        : base(address)
    {
        _fileSystemId = fileSystemId;
    }

    public bool HasReportedBlocks { get; set; }

    public HashSet<Guid> Blocks { get { return _blocks; } }

    public HashSet<Guid> PendingBlocks { get { return _pendingBlocks; } }

    public DateTime LastContactUtc { get; set; }

    public long DiskSpaceUsed { get; set; }

    public long DiskSpaceFree { get; set; }

    public long DiskSpaceTotal { get; set; }

    public void AddResponseForNextHeartbeat(HeartbeatResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);

        lock (_pendingResponses)
        {
            _pendingResponses.Add(response);
        }
    }

    public void AddBlockToDelete(Guid blockID)
    {
        lock (_pendingResponses)
        {
            var response = (from r in _pendingResponses
                            let dr = r as DeleteBlocksHeartbeatResponse
                            where dr != null
                            select dr).SingleOrDefault();
            if (response == null)
            {
                _pendingResponses.Add(new DeleteBlocksHeartbeatResponse(_fileSystemId, new[] { blockID }));
            }
            else
            {
                response.Blocks.Add(blockID);
            }
        }
    }

    public HeartbeatResponse[] GetAndClearPendingResponses()
    {
        lock (_pendingResponses)
        {
            var result = _pendingResponses.ToArray();
            _pendingResponses.Clear();
            return result;
        }
    }
}
