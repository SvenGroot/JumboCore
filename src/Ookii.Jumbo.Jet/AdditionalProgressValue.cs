// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// The value of an additional progress counter.
    /// </summary>
    [Serializable]
    public sealed class AdditionalProgressValue
    {
        /// <summary>
        /// Gets or sets the name of the source of the progress counter.
        /// </summary>
        /// <value>The name of the source.</value>
        public string? SourceName { get; set; }

        /// <summary>
        /// Gets or sets the progress.
        /// </summary>
        /// <value>The progress.</value>
        public float Progress { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}: {1:P1}", SourceName, Progress);
        }
    }
}
