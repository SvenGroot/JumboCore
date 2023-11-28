// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Provides constants for use with implementations of <see cref="Ookii.Jumbo.IO.IPartitioner{T}"/>.
/// </summary>
/// <remarks>
/// <para>
///   Partitioners for use with Jumbo Jet should allow the user to specify an equality comparer using the stage settings
///   using the settings key specified by <see cref="EqualityComparerSetting"/>.
/// </para>
/// </remarks>
public static class PartitionerConstants
{
    /// <summary>
    /// The name of the setting in <see cref="Jobs.StageConfiguration.StageSettings"/> that specifies the <see cref="IEqualityComparer{T}"/>
    /// to use. If this setting is not specified, <see cref="EqualityComparer{T}.Default"/> will be used.
    /// </summary>
    public const string EqualityComparerSetting = "Partitioner.EqualityComparer";
}
