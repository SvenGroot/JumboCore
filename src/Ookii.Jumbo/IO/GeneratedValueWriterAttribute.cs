using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Indicates that the <see cref="IValueWriter{T}"/> implementation for a type should be created
/// with source generation.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class GeneratedValueWriterAttribute : Attribute
{
}
