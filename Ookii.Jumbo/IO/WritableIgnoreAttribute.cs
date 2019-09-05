// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
