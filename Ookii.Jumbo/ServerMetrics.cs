// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides metrics about a data or task server.
    /// </summary>
    [Serializable]
    public class ServerMetrics
    {
        /// <summary>
        /// Gets or sets the address of the server.
        /// </summary>
        /// <value>
        /// The address of the server.
        /// </value>
        public ServerAddress Address { get; set; }

        /// <summary>
        /// Gets or sets the ID of the rack of the server.
        /// </summary>
        /// <value>
        /// The rack ID of the server, or <see langword="null"/> if rack-awareness isn't used or this server is in the default rack.
        /// </value>
        public string RackId { get; set; }

        /// <summary>
        /// Gets or sets the time of the last heartbeat sent to the name server (for data servers) or job server (for task servers).
        /// </summary>
        /// <value>
        /// The UTC date and time of the last heartbeat sent.
        /// </value>
        public DateTime LastContactUtc { get; set; }

        /// <summary>
        /// Gets a string representation of the current <see cref="ServerMetrics"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="ServerMetrics"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.CurrentCulture, "{0}; Rack: {1}; Last contact: {2:0.0}s ago", Address, RackId ?? "(unknown)", (DateTime.UtcNow - LastContactUtc).TotalSeconds);
        }
    }
}
