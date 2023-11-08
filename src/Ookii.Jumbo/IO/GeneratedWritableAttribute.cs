using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Indicates that the <see cref="IWritable"/> implementation for a type should be created with
/// source generation.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class GeneratedWritableAttribute : Attribute
{
    /// <summary>
    /// Gets or sets a value which indicates whether the generated methods will be virtual.
    /// </summary>
    /// <value>
    /// <see langword="true"/> to generate virtual methods; otherwise, <see langword="false"/>.
    /// The default value is <see langword="false"/>.
    /// </value>
    /// <remarks>
    /// <para>
    ///   This has no effect if the base class already implemented the <see cref="IWritable"/>
    ///   interface, in which case the generated code will always attempt to overwrite the base
    ///   class methods.
    /// </para>
    /// </remarks>
    public bool Virtual { get; set; }
}
