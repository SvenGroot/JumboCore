// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides the types of compression supported by Jumbo.
    /// </summary>
    public enum CompressionType
    {
        /// <summary>
        /// Don't use compression.
        /// </summary>
        None,
        /// <summary>
        /// Use the <see cref="System.IO.Compression.GZipStream"/> class for compression.
        /// </summary>
        GZip
    }
}
