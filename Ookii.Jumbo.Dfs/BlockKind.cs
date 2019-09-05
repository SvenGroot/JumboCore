// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Specifies what kind of blocks to return from <see cref="INameServerClientProtocol.GetBlocks"/>.
    /// </summary>
    public enum BlockKind
    {
        /// <summary>
        /// All regular, non-pending blocks.
        /// </summary>
        Normal,
        /// <summary>
        /// All blocks that are insufficiently replicated.
        /// </summary>
        UnderReplicated,
        /// <summary>
        /// All blocks that are pending.
        /// </summary>
        Pending
    }
}
