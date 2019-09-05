// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Represents the type of the runtime being used.
    /// </summary>
    public enum RuntimeEnvironmentType
    {
        /// <summary>
        /// The runtime is the Microsoft .Net Framework.
        /// </summary>
        DotNet,
        /// <summary>
        /// The runtime is Mono.
        /// </summary>
        Mono
    }
}
