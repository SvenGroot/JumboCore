﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Heartbeat data informing the server of the status 
    /// </summary>
    [Serializable]
    public class InitialStatusJetHeartbeatData : JetHeartbeatData 
    {
        /// <summary>
        /// Gets or sets the maximum number of tasks that this task server will accept.
        /// </summary>
        public int TaskSlots { get; set; }

        /// <summary>
        /// Gets or sets the port on which the task server accepts connections to download files for the
        /// file input channel.
        /// </summary>
        public int FileServerPort { get; set; }
    }
}
