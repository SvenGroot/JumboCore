// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.IO;

/// <summary>
/// Provides methods for describing a task input.
/// </summary>
public interface ITaskInput : IWritable
{
    /// <summary>
    /// Gets a list of host names of nodes for which this task's input is local.
    /// </summary>
    /// <value>
    /// The locations, or <see langword="null"/> if the input doesn't use locality.
    /// </value>
    /// <remarks>
    /// The <see cref="IWritable"/> implementation doesn't need to serialize this array; it will be stored separately.
    /// </remarks>
    ICollection<string>? Locations { get; }
}
