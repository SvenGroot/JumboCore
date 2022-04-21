// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    public class CustomerOrder : Writable<CustomerOrder>, IComparable<CustomerOrder>
    {
        public int CustomerId { get; set; }
        public int OrderId { get; set; }
        public int ItemId { get; set; }
        public string Name { get; set; }

        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "CustomerId = {0}, OrderId = {1}, ItemId = {2}, Name = {3}", CustomerId, OrderId, ItemId, Name);
        }

        public override bool Equals(object obj)
        {
            CustomerOrder other = obj as CustomerOrder;
            if (other == null)
                return false;
            return CustomerId == other.CustomerId && OrderId == other.OrderId && ItemId == other.ItemId && Name == other.Name;
        }

        public override int GetHashCode()
        {
            return CustomerId.GetHashCode();
        }

        #region IComparable<CustomerOrder> Members

        public int CompareTo(CustomerOrder other)
        {
            int result = CustomerId - other.CustomerId;
            if (result == 0)
                result = OrderId - other.OrderId;
            if (result == 0)
                result = ItemId - other.ItemId;
            if (result == 0)
                result = StringComparer.Ordinal.Compare(Name, other.Name);
            return result;
        }

        #endregion
    }
}
