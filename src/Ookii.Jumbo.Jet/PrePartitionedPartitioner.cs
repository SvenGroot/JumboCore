// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

sealed class PrepartitionedPartitioner<T> : IPartitioner<T>
    where T : notnull
{
    private int _currentPartition;

    public int Partitions { get; set; }

    public int CurrentPartition
    {
        get { return _currentPartition; }
        set
        {
            if (value < 0 || value >= Partitions)
            {
                throw new ArgumentOutOfRangeException(nameof(value));
            }

            _currentPartition = value;
        }
    }


    public int GetPartition(T value)
    {
        return CurrentPartition;
    }
}
