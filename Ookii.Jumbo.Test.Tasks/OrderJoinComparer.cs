// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    /// <summary>
    /// Provides <see cref="Order"/> comparisons based on <see cref="Order.CustomerId"/> which is needed for the join.
    /// </summary>
    public class OrderJoinComparer : IRawComparer<Order>, IEqualityComparer<Order>
    {
        public int Compare(byte[] buffer1, int offset1, int count1, byte[] buffer2, int offset2, int count2)
        {
            // customer ID is the second field in the binary representation, hence offset + 4.
            int customerId1 = LittleEndianBitConverter.ToInt32(buffer1, offset1 + 4);
            int customerId2 = LittleEndianBitConverter.ToInt32(buffer2, offset2 + 4);
            return customerId1.CompareTo(customerId2);
        }

        public int Compare(Order x, Order y)
        {
            return x.CustomerId.CompareTo(y.CustomerId);
        }

        public bool Equals(Order x, Order y)
        {
            if (x == y)
                return true;
            else if (x == null || y == null)
                return false;
            else
                return x.CustomerId == y.CustomerId;
        }

        public int GetHashCode(Order obj)
        {
            return obj.CustomerId.GetHashCode();
        }

    }
}
