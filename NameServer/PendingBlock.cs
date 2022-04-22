// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Threading;

namespace NameServerApplication
{
    class PendingBlock
    {
        private int _commitCount;

        public PendingBlock(BlockInfo block)
        {
            if (block == null)
                throw new ArgumentNullException(nameof(block));
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
