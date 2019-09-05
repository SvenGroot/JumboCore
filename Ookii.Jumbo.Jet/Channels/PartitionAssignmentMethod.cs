// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Indicates how initial partition assignment is done if the channel has multiple partitions per task.
    /// </summary>
    public enum PartitionAssignmentMethod
    {
        /// <summary>
        /// Each task gets a linear sequence of partitions, e.g. task 1 gets partitions 1, 2 and 3, task 2 gets partitions 4, 5 and 6, and so forth.
        /// </summary>
        Linear,
        /// <summary>
        /// The partitions are striped across the tasks, e.g. task 1 gets partitions 1, 3, and 5, and task 2 gets partitions 2, 4, and 6.
        /// </summary>
        Striped
    }
}
