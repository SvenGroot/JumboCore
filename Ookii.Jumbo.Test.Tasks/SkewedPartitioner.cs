// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    // Important! Must have at least 4 partitions for this partitioner to work
    public class SkewedPartitioner<T> : IPartitioner<T>
    {
        public int Partitions { get; set; }

        public int GetPartition(T value)
        {
            // Assign 90% of the data to the first partition.
            if (value.GetHashCode() % 10 < 9)
                return 0;
            else
                return value.GetHashCode() % (Partitions - 1) + 1;
        }
    }
}
