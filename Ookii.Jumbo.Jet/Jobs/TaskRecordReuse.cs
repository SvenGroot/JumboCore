// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// The record reuse option used by a task.
    /// </summary>
    public enum TaskRecordReuse
    {
        /// <summary>
        /// The task doesn't allow record reuse.
        /// </summary>
        NotAllowed,
        /// <summary>
        /// The task allows record reuse.
        /// </summary>
        Allowed,
        /// <summary>
        /// The task allows record reuse only if its output allows record reuse.
        /// </summary>
        PassThrough
    }
}
