// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace NameServerApplication
{
    class PendingBlock
    {
        private int _commitCount;

        public PendingBlock(BlockInfo block)
        {
            if( block == null )
                throw new ArgumentNullException("block");
            Block = block;
        }

        public int CommitCount { get { return _commitCount; } }
        public BlockInfo Block { get; private set; }

        public int IncrementCommit()
        {
            return Interlocked.Increment(ref _commitCount);
        }
    }
}
