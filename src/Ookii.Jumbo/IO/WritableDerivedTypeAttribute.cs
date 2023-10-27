using System;

namespace Ookii.Jumbo.IO;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class WritableDerivedTypeAttribute : System.Attribute
{
    public WritableDerivedTypeAttribute(Type derivedType)
    {
        ArgumentNullException.ThrowIfNull(derivedType);
        DerivedType = derivedType;
    }

    public Type DerivedType { get; private set; }
}
