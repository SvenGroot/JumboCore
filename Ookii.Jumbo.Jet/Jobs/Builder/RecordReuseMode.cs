using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// The way the record reuse attribute is applied to tasks that are generated from a
    /// delegate function by the <see cref="JobBuilder"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This allows you to specify the record reuse mode when using an anonymous delegate or lambda as
    ///   the target method.
    /// </para>
    /// </remarks>
    public enum RecordReuseMode
    {
        /// <summary>
        /// The record reuse attribute is copied from the delegate's target method.
        /// </summary>
        Default,
        /// <summary>
        /// The record reuse attribute is never applied.
        /// </summary>
        DoNotAllow,
        /// <summary>
        /// The record reuse attribute is always applied, with <see cref="AllowRecordReuseAttribute.PassThrough"/> set to <see langword="false"/>.
        /// </summary>
        Allow,
        /// <summary>
        /// The record reuse attribute is always applied, with <see cref="AllowRecordReuseAttribute.PassThrough"/> set to <see langword="true"/>.
        /// </summary>
        PassThrough
    }
}
