// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// The current status of a task attempt.
    /// </summary>
    public enum TaskAttemptStatus
    {
        /// <summary>
        /// The task has not yet been started.
        /// </summary>
        NotStarted,
        /// <summary>
        /// The task is running.
        /// </summary>
        Running,
        /// <summary>
        /// The task has completed successfully.
        /// </summary>
        Completed,
        /// <summary>
        /// Task execution encountered an error.
        /// </summary>
        Error
    }
}
