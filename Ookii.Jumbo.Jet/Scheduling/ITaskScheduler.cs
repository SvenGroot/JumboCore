﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Scheduling
{
    /// <summary>
    /// Interface for Jumbo Jet task schedulers.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Implement this interface if you want to customize the scheduling behavior of Jumbo Jet. Use the <see cref="JobServerConfigurationElement.Scheduler"/>
    ///   configuration property to specify which scheduler to use.
    /// </para>
    /// </remarks>
    public interface ITaskScheduler
    {
        /// <summary>
        /// Performs a scheduling pass.
        /// </summary>
        /// <param name="jobs">The current active jobs.</param>
        void ScheduleTasks(IEnumerable<IJobInfo> jobs);
    }
}
