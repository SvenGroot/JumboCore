// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth;

/// <summary>
/// Item in the feature and group list.
/// </summary>
[GeneratedWritable]
public partial class FGListItem : IComparable<FGListItem>
{
    /// <summary>
    /// Gets or sets the feature.
    /// </summary>
    /// <value>The feature.</value>
    public Utf8String? Feature { get; set; }
    /// <summary>
    /// Gets or sets the support.
    /// </summary>
    /// <value>The support.</value>
    public int Support { get; set; }
    /// <summary>
    /// Gets or sets the group id.
    /// </summary>
    /// <value>The group id.</value>
    public int GroupId { get; set; }

    /// <summary>
    /// Returns a <see cref="System.String"/> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="System.String"/> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        return string.Format(CultureInfo.InvariantCulture, "[{0}, Support={1}, Group ID={2}]", Feature, Support, GroupId);
    }

    /// <summary>
    /// Compares to another <see cref="FGListItem"/>.
    /// </summary>
    /// <param name="other">The other.</param>
    /// <returns>The comparison result.</returns>
    public int CompareTo(FGListItem? other)
    {
        if (other == null)
        {
            return 1;
        }

        int result = other.Support.CompareTo(Support); // Sort on support descending
        if (result == 0)
        {
            result = Feature!.CompareTo(other.Feature); // And feature ascending
        }
        return result;
    }
}
