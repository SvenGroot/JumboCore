// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Xml.Serialization;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Jobs;

/// <summary>
/// Provides information about additional progress counters in the <see cref="JobConfiguration"/>.
/// </summary>
[GeneratedWritable] // Binary serializable because it's used in JobStatus
[XmlType(Namespace = JobConfiguration.XmlNamespace)] // XML serializable for JobConfiguration.
public partial class AdditionalProgressCounter
{
    /// <summary>
    /// Gets or sets the name of the type that reports the additional progress.
    /// </summary>
    /// <value>The name of the type.</value>
    public string? TypeName { get; set; }

    /// <summary>
    /// Gets or sets the display name of the progress counter.
    /// </summary>
    /// <value>The display name.</value>
    public string? DisplayName { get; set; }

    /// <summary>
    /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
    /// </summary>
    /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
    /// <returns>
    /// 	<see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
    /// </returns>
    /// <exception cref="System.NullReferenceException">
    /// The <paramref name="obj"/> parameter is null.
    /// </exception>
    public override bool Equals(object? obj)
    {
        var counter = obj as AdditionalProgressCounter;
        if (counter == null)
            return base.Equals(obj);
        else
            return counter.TypeName == TypeName;
    }

    /// <summary>
    /// Returns a hash code for this instance.
    /// </summary>
    /// <returns>
    /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
    /// </returns>
    public override int GetHashCode()
    {
        return TypeName == null ? 0 : TypeName.GetHashCode(StringComparison.Ordinal);
    }
}
