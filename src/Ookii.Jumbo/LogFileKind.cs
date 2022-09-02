// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo
{
    /// <summary>
    /// The type of log file of a process.
    /// </summary>
    public enum LogFileKind
    {
        /// <summary>
        /// The log file created by log4net.
        /// </summary>
        Log,
        /// <summary>
        /// The standard output.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Std")]
        StdOut,
        /// <summary>
        /// The standard error.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Std")]
        StdErr
    }
}
