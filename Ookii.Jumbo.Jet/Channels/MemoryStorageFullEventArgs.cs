// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Provides data for the <see cref="IInputChannel.MemoryStorageFull"/> event.
    /// </summary>
    public class MemoryStorageFullEventArgs : EventArgs
    {
        private readonly long _spaceNeeded;

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryStorageFullEventArgs"/> class.
        /// </summary>
        /// <param name="spaceNeeded">The space that must be freed before the memory storage manager can satisfy the current request.</param>
        public MemoryStorageFullEventArgs(long spaceNeeded)
        {
            _spaceNeeded = spaceNeeded;
            CancelWaiting = true;
        }

        /// <summary>
        /// Gets the amount space that must be freed before the memory storage manager can satisfy the current request.
        /// </summary>
        /// <value>
        /// The amount space that must be freed before the memory storage manager can satisfy the current request.
        /// </value>
        public long SpaceNeeded
        {
            get { return _spaceNeeded; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the memory storage manager should wait for memory to become available.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if the memory storage manager should not wait; otherwise, <see langword="false" />.
        /// The default value is <see langword="true"/>.
        /// </value>
        public bool CancelWaiting { get; set; }
    }
}
