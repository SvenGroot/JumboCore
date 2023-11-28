// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth;

/// <summary>
/// A frequent pattern consisting of sorted item IDs. Used as intermediate data format.
/// </summary>
public class MappedFrequentPattern : IWritable, IComparable<MappedFrequentPattern>, IEquatable<MappedFrequentPattern>
{
    private int[] _items;
    private ReadOnlyCollection<int> _itemsReadOnlyWrapper;
    private int? _hashCode;

    /// <summary>
    /// Initializes a new instance of the <see cref="MappedFrequentPattern"/> class.
    /// </summary>
    /// <param name="items">The items of the patterns; must be sorted by item ID.</param>
    /// <param name="support">The support of the pattern.</param>
    public MappedFrequentPattern(IEnumerable<int> items, int support)
    {
        ArgumentNullException.ThrowIfNull(items);
        _items = items.ToArray();
        _itemsReadOnlyWrapper = new ReadOnlyCollection<int>(_items);
        Support = support;
    }

    /// <summary>
    /// Gets the items.
    /// </summary>
    /// <value>The items.</value>
    public ReadOnlyCollection<int> Items
    {
        get { return _itemsReadOnlyWrapper; }
    }

    /// <summary>
    /// Gets or sets the support.
    /// </summary>
    /// <value>The support.</value>
    public int Support { get; private set; }

    /// <summary>
    /// Returns a <see cref="System.String"/> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="System.String"/> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        return string.Format("{0}:{1}", Items.ToDelimitedString(","), Support);
    }

    /// <summary>
    /// Compares this instance to another <see cref="MappedFrequentPattern"/> instance.
    /// </summary>
    /// <param name="other">The <see cref="MappedFrequentPattern"/> to compare to..</param>
    /// <returns>Less than 0 if this instance is less than <paramref name="other"/>, 0 if they are equal, or 1 if this instance is greater than <paramref name="other"/>.</returns>
    /// <remarks>
    /// <para>
    ///   The comparison is done based on the support and then number of items. The values of the items are not considered.
    /// </para>
    /// </remarks>
    public int CompareTo(MappedFrequentPattern? other)
    {
        if (other == null)
        {
            return 1;
        }

        if (Support == other.Support)
        {
            return _items.Length == other._items.Length ? 0 :
                (_items.Length < other._items.Length ? -1 : 1);
        }
        else
        {
            return Support < other.Support ? -1 : 1;
        }
    }

    /// <summary>
    /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
    /// </summary>
    /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
    /// <returns>
    /// 	<see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
    /// </returns>
    public override bool Equals(object? obj)
    {
        return Equals(obj as MappedFrequentPattern);
    }

    /// <summary>
    /// Determines whether the specified <see cref="MappedFrequentPattern"/> is equal to this instance.
    /// </summary>
    /// <param name="other">The <see cref="MappedFrequentPattern"/> to compare with this instance.</param>
    /// <returns>
    /// 	<see langword="true"/> if the specified <see cref="MappedFrequentPattern"/> is equal to this instance; otherwise, <see langword="false"/>.
    /// </returns>
    public bool Equals(MappedFrequentPattern? other)
    {
        if (other == null)
        {
            return false;
        }

        if (this == other)
        {
            return true;
        }

        if (Support == other.Support && _items.Length == other._items.Length)
        {
            return _items.SequenceEqual(other._items);
        }
        return false;
    }

    /// <summary>
    /// Returns a hash code for this instance.
    /// </summary>
    /// <returns>
    /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
    /// </returns>
    public override int GetHashCode()
    {
        // We're caching hash code on the assumption that the instance is immutable; we should actually ensure this.
        if (_hashCode == null)
        {
            int result = _items.GetSequenceHashCode();
            result = 31 * result + Support.GetHashCode();
            result = 31 * result + _items.Length;
            _hashCode = result;
            return result;
        }
        return _hashCode.Value;
    }

    /// <summary>
    /// Writes the object to the specified writer.
    /// </summary>
    /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
    public void Write(BinaryWriter writer)
    {
        WritableUtility.Write7BitEncodedInt32(writer, _items.Length);
        foreach (int item in _items)
        {
            writer.Write(item);
        }

        writer.Write(Support);
    }

    /// <summary>
    /// Reads the object from the specified reader.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
    public void Read(BinaryReader reader)
    {
        int length = WritableUtility.Read7BitEncodedInt32(reader);
        _items = new int[length];
        for (int x = 0; x < length; ++x)
        {
            _items[x] = reader.ReadInt32();
        }
        _itemsReadOnlyWrapper = new ReadOnlyCollection<int>(_items);
        Support = reader.ReadInt32();
    }

    internal bool IsSubpatternOf(MappedFrequentPattern pattern2)
    {
        int[] pattern = _items;
        int[] otherPattern = pattern2._items;
        int otherLength = otherPattern.Length;
        if (pattern.Length > otherLength)
        {
            return false;
        }
        int i = 0;
        int otherI = 0;
        int length = pattern.Length;
        while (i < length && otherI < otherLength)
        {
            if (otherPattern[otherI] == pattern[i])
            {
                otherI++;
                i++;
            }
            else if (otherPattern[otherI] > pattern[i])
            {
                otherI++;
            }
            else
            {
                return false;
            }
        }
        return otherI != otherLength || i == length;
    }

}
