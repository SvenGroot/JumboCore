// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Test.Tasks
{
    public class Order : IWritable, ICloneable
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public int ItemId { get; set; }

        public override bool Equals(object obj)
        {
            Order other = obj as Order;
            if (other == null)
                return false;
            return Id == other.Id && CustomerId == other.CustomerId && ItemId == other.ItemId;
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }

        public object Clone()
        {
            return MemberwiseClone();
        }

        public void Write(System.IO.BinaryWriter writer)
        {
            writer.Write(Id);
            writer.Write(CustomerId);
            writer.Write(ItemId);
        }

        public void Read(System.IO.BinaryReader reader)
        {
            Id = reader.ReadInt32();
            CustomerId = reader.ReadInt32();
            ItemId = reader.ReadInt32();
        }
    }
}
