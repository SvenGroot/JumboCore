// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    [RawComparer(typeof(CustomerComparer))]
    public class Customer : Writable<Customer>, IComparable<Customer>, ICloneable
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override bool Equals(object obj)
        {
            Customer other = obj as Customer;
            if( other == null )
                return false;
            return Id == other.Id && Name == other.Name;
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }

        #region ICloneable Members

        public object Clone()
        {
            return MemberwiseClone();
        }

        #endregion

        #region IComparable<Customer> Members

        public int CompareTo(Customer other)
        {
            return Id.CompareTo(other.Id);
        }

        #endregion
    }
}
