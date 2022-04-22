// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Indicates that a property on class inheriting from <see cref="Writable{T}"/> should not be serialized.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class WritableIgnoreAttribute : Attribute
    {
    }
}
