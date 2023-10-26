// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Indicates that a property on class using the <see cref="GeneratedWritableAttribute"/>
    /// attribute should not be serialized.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class WritableIgnoreAttribute : Attribute
    {
    }
}
