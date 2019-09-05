// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Base class for heartbeat responses from the job server to the task servers.
    /// </summary>
    [Serializable]
    public class JetHeartbeatResponse
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JetHeartbeatResponse"/> class with the specified command.
        /// </summary>
        /// <param name="command">The command to send to the task server.</param>
        public JetHeartbeatResponse(TaskServerHeartbeatCommand command)
        {
            Command = command;
        }

        /// <summary>
        /// Gets the command the job server sent to the task server.
        /// </summary>
        public TaskServerHeartbeatCommand Command { get; private set; }
    }
}
