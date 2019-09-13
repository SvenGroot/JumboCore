// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Attribute for task classes that indicates that the input record reader may reuse the same
    /// object instance for every record.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public sealed class AllowRecordReuseAttribute : Attribute
    {
        /// <summary>
        /// Gets or sets a value that indicates whether the task will pass on the instances it receives
        /// from its input to its output.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   If the task passes the same instance it gets as input to its output, and the task uses
        ///   a pipeline output channel, record reuse is only allowed if the output tasks of the
        ///   pipeline channel also allow record reuse. Set this property to <see langword="true"/>
        ///   to indicate Jumbo Jet should verify any output tasks on a pipeline channel also
        ///   have the <see cref="AllowRecordReuseAttribute"/> attribute before allowing
        ///   record reuse.
        /// </para>
        /// </remarks>
        public bool PassThrough { get; set; }
    }
}
