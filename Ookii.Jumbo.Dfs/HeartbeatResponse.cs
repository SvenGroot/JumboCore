// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents the response sent by the NameServer to a Heartbeat message from the DataServer.
    /// </summary>
    [Serializable]
    public class HeartbeatResponse
    {
        /// <summary>
        /// Initializes a new innstance of the <see cref="HeartbeatResponse" /> class with the specified command.
        /// </summary>
        /// <param name="fileSystemId">The file system id.</param>
        /// <param name="command">The <see cref="DataServerHeartbeatCommand" /> to send to the server.</param>
        public HeartbeatResponse(Guid fileSystemId, DataServerHeartbeatCommand command)
        {
            FileSystemId = fileSystemId;
            Command = command;
        }


        /// <summary>
        /// Gets the command that the NameServer is giving to the DataServer.
        /// </summary>
        /// <value>
        /// A <see cref="DataServerHeartbeatCommand"/> value that indicates the command issued to the DataServer.
        /// </value>
        public DataServerHeartbeatCommand Command { get; private set; }

        /// <summary>
        /// Gets the file system ID.
        /// </summary>
        /// <value>
        /// A <see cref="Guid"/> that uniquely identifies this file system.
        /// </value>
        public Guid FileSystemId { get; private set; }
    }
}
