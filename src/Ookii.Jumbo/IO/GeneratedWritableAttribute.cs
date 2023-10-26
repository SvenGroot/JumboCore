using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Indicates that the IWritable implementation for a type should be create with source generation.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class GeneratedWritableAttribute : Attribute
{
}
