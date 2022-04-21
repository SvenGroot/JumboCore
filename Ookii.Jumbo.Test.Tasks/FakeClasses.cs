using System;
using System.Collections.Generic;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    public class FakePartitioner<T> : IPartitioner<T>
    {
        public int Partitions
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public int GetPartition(T value)
        {
            throw new NotImplementedException();
        }
    }

    public class FakeComparer<T> : IComparer<T>
    {
        public int Compare(T x, T y)
        {
            throw new NotImplementedException();
        }
    }

    public class FakeEqualityComparer<T> : IEqualityComparer<T>
    {
        public bool Equals(T x, T y)
        {
            throw new NotImplementedException();
        }

        public int GetHashCode(T obj)
        {
            throw new NotImplementedException();
        }
    }

    public class FakeJoinComparer<T> : IRawComparer<T>, IEqualityComparer<T>
    {
        public int Compare(T x, T y)
        {
            throw new NotImplementedException();
        }

        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            throw new NotImplementedException();
        }

        public bool Equals(T x, T y)
        {
            throw new NotImplementedException();
        }

        public int GetHashCode(T obj)
        {
            throw new NotImplementedException();
        }
    }

    public class FakeRawComparer<T> : IRawComparer<T>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            throw new NotImplementedException();
        }

        public int Compare(T x, T y)
        {
            throw new NotImplementedException();
        }
    }


    public class FakeCombiner<T> : ITask<T, T>
    {
        public void Run(RecordReader<T> input, RecordWriter<T> output)
        {
            throw new NotImplementedException();
        }
    }

    [InputType(typeof(double)), InputType(typeof(int))]
    public class FakeInnerJoinRecordReader : InnerJoinRecordReader<double, int, Utf8String>
    {
        public FakeInnerJoinRecordReader()
            : base(null, 0, false, 0, CompressionType.None)
        {
        }

        protected override int Compare(double outer, int inner)
        {
            throw new NotImplementedException();
        }

        protected override Utf8String CreateJoinResult(Utf8String result, double outer, int inner)
        {
            throw new NotImplementedException();
        }
    }
}
