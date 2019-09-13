// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    [InputType(typeof(Customer)), InputType(typeof(Order))]
    public sealed class CustomerOrderJoinRecordReader : InnerJoinRecordReader<Customer, Order, CustomerOrder>
    {
        public CustomerOrderJoinRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
        }

        protected override CustomerOrder CreateJoinResult(CustomerOrder result, Customer outer, Order inner)
        {
            result.CustomerId = outer.Id;
            result.OrderId = inner.Id;
            result.Name = outer.Name;
            result.ItemId = inner.ItemId;
            return result;
        }

        protected override int Compare(Customer outer, Order inner)
        {
            return outer.Id - inner.CustomerId;
        }
    }
}
