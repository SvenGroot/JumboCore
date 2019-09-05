using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Indicates how the scheduler should assign tasks to servers.
    /// </summary>
    public enum SchedulingMode
    {
        /// <summary>
        /// The scheduler will use the job server's default scheduling mode.
        /// </summary>
        Default,
        /// <summary>
        /// The scheduler will attempt to spread the workload over as many servers as possible.
        /// </summary>
        MoreServers,
        /// <summary>
        /// The scheduler will attempt to use as few servers as possible
        /// </summary>
        FewerServers,
        /// <summary>
        /// The scheduler will attempt to minimize the number of non-local tasks. This value is not valid for tasks that do not read data input.
        /// </summary>
        OptimalLocality
    }
}
