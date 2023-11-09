using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Specifies a derived type that is allowed for serialization and deserialization with the
/// <see cref="PolymorphicValueWriter{T}"/> class.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class WritableDerivedTypeAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WritableDerivedTypeAttribute"/> class.
    /// </summary>
    /// <param name="derivedType">The derived type.</param>
    public WritableDerivedTypeAttribute(Type derivedType)
    {
        ArgumentNullException.ThrowIfNull(derivedType);
        DerivedType = derivedType;
    }

    /// <summary>
    /// Gets the derived type.
    /// </summary>
    /// <value>
    /// The <see cref="Type"/> of the derived type.
    /// </value>
    public Type DerivedType { get; }
}
