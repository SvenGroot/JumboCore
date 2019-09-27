// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    sealed class PrepartitionedPartitioner<T> : IPartitioner<T>
    {
        private int _currentPartition;

        public int Partitions { get; set; }

        public int CurrentPartition
        {
            get { return _currentPartition; }
            set 
            {
                if( value < 0 || value >= Partitions )
                    throw new ArgumentOutOfRangeException(nameof(value));
                _currentPartition = value; 
            }
        }
        

        public int GetPartition(T value)
        {
            return CurrentPartition;
        }
    }
}
