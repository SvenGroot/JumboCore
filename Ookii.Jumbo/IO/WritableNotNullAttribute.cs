// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Indicates a property on a class that inherits from <see cref="Writable{T}"/> will not be <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// If this is set, the <see cref="IWritable.Write"/> method will throw an exception if the
    /// property is <see langword="null"/>. A boolean to indicate <see langword="null"/> values will not be written to the
    /// stream, and the <see cref="IWritable.Read"/> method will not check it.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    public sealed class WritableNotNullAttribute : Attribute
    {
    }
}
