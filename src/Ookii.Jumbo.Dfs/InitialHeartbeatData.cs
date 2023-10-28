// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Heartbeat data sent by the name server the first time it sends a heartbeat to the server.
    /// </summary>
    [GeneratedWritable]
    public partial class InitialHeartbeatData : HeartbeatData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InitialHeartbeatData"/> class.
        /// </summary>
        /// <param name="fileSystemId">The file system id.</param>
        public InitialHeartbeatData(Guid fileSystemId)
        {
            FileSystemId = fileSystemId;
        }

        /// <summary>
        /// Gets the file system id.
        /// </summary>
        /// <value>
        /// The file system id.
        /// </value>
        public Guid FileSystemId { get; private set; }
    }
}
