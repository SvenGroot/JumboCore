// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Interface for tasks, multi record readers and channels that report additional progress.
    /// </summary>
    /// <remarks>
    /// Use the <see cref="AdditionalProgressCounterAttribute"/> attribute to specify a name for the counter.
    /// </remarks>
    public interface IHasAdditionalProgress
    {
        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        /// <remarks>
        /// This property must be thread safe.
        /// </remarks>
        float AdditionalProgress { get; }
    }
}
