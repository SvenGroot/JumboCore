// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides information about task servers.
    /// </summary>
    [Serializable]
    public class TaskServerMetrics : ServerMetrics
    {
        /// <summary>
        /// Gets or sets the maximum number of tasks that this server can run.
        /// </summary>
        public int TaskSlots { get; set; }

        /// <summary>
        /// Returns a string representation of the current <see cref="TaskServerMetrics"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="TaskServerMetrics"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0}; task slots: {1}", base.ToString(), TaskSlots);
        }
    }
}
