// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Samples;

/// <summary>
/// A wrapper around the <see cref="StringComparer.OrdinalIgnoreCase"/>, so it can be used with <see langword="typeof"/>.
/// </summary>
class OrdinalIgnoreCaseStringComparer : StringComparer
{
    /// <summary>
    /// When overridden in a derived class, compares two strings and returns an indication of their relative sort order.
    /// </summary>
    /// <param name="x">A string to compare to <paramref name="y" />.</param>
    /// <param name="y">A string to compare to <paramref name="x" />.</param>
    /// <returns>
    /// A signed integer that indicates the relative values of <paramref name="x" /> and <paramref name="y" />, as shown in the following table.ValueMeaningLess than zero<paramref name="x" /> is less than <paramref name="y" />.-or-<paramref name="x" /> is null.Zero<paramref name="x" /> is equal to <paramref name="y" />.Greater than zero<paramref name="x" /> is greater than <paramref name="y" />.-or-<paramref name="y" /> is null.
    /// </returns>
    public override int Compare(string? x, string? y)
    {
        return OrdinalIgnoreCase.Compare(x, y);
    }

    /// <summary>
    /// When overridden in a derived class, indicates whether two strings are equal.
    /// </summary>
    /// <param name="x">A string to compare to <paramref name="y" />.</param>
    /// <param name="y">A string to compare to <paramref name="x" />.</param>
    /// <returns>
    /// <see langword="true"/> if <paramref name="x" /> and <paramref name="y" /> refer to the same object, or <paramref name="x" /> and <paramref name="y" /> are equal; otherwise, <see langword="false"/>.
    /// </returns>
    public override bool Equals(string? x, string? y)
    {
        return OrdinalIgnoreCase.Equals(x, y);
    }

    /// <summary>
    /// Returns a hash code for this instance.
    /// </summary>
    /// <param name="obj">The obj.</param>
    /// <returns>
    /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
    /// </returns>
    public override int GetHashCode(string obj)
    {
        return OrdinalIgnoreCase.GetHashCode(obj);
    }
}
