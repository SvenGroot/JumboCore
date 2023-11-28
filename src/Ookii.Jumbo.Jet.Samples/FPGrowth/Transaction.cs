// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth;

/// <summary>
/// Used as intermediate type for the PFP growth job.
/// </summary>
public class Transaction : IWritable, ITransaction
{
    private int[] _items = Array.Empty<int>();

    /// <summary>
    /// Gets or sets the items.
    /// </summary>
    /// <value>The items.</value>
    public int[] Items
    {
        get { return _items; }
        set { _items = value; }
    }

    /// <summary>
    /// Gets or sets the length.
    /// </summary>
    /// <value>The length.</value>
    public int Length { get; set; }

    /// <summary>
    /// Writes the object to the specified writer.
    /// </summary>
    /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
    public void Write(BinaryWriter writer)
    {
        if (_items == null)
        {
            WritableUtility.Write7BitEncodedInt32(writer, 0);
        }
        else
        {
            WritableUtility.Write7BitEncodedInt32(writer, Length);
            for (int x = 0; x < Length; ++x)
            {
                writer.Write(_items[x]);
            }
        }
    }

    /// <summary>
    /// Reads the object from the specified reader.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
    public void Read(BinaryReader reader)
    {
        Length = WritableUtility.Read7BitEncodedInt32(reader);
        if (_items == null || _items.Length < Length)
        {
            _items = new int[Length];
        }

        for (int x = 0; x < Length; ++x)
        {
            _items[x] = reader.ReadInt32();
        }
    }

    /// <summary>
    /// Returns a <see cref="System.String"/> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="System.String"/> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        return "{ " + Items.Take(Length).ToDelimitedString() + " }";
    }

    IEnumerable<int> ITransaction.Items
    {
        get { return _items.Take(Length); }
    }

    int ITransaction.Count
    {
        get { return 1; }
    }
}
