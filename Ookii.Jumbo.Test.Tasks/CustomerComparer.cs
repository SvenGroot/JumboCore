using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    class CustomerComparer : IRawComparer<Customer>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            int id1 = LittleEndianBitConverter.ToInt32(buffer1, offset1);
            int id2 = LittleEndianBitConverter.ToInt32(buffer2, offset2);
            return id1.CompareTo(id2);
        }

        public int Compare(Customer x, Customer y)
        {
            return Comparer<Customer>.Default.Compare(x, y);
        }
    }
}
