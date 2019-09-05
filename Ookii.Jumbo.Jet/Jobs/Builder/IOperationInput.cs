﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents the input to an operation, which is either a data input or another operation.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Only implement this interface if you're also implementing <see cref="IJobBuilderOperation"/>. Data inputs
    ///   are the only non-operation inputs.
    /// </para>
    /// </remarks>
    public interface IOperationInput
    {
        /// <summary>
        /// Gets the type of the records provided by this input.
        /// </summary>
        /// <value>
        /// The type of the record.
        /// </value>
        Type RecordType { get; }
    }
}
