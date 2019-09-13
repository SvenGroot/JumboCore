﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Interface for transactions used by the FP-growth algorithm.
    /// </summary>
    public interface ITransaction
    {
        /// <summary>
        /// Gets the items.
        /// </summary>
        /// <value>The items.</value>
        IEnumerable<int> Items { get; }
        /// <summary>
        /// Gets the number of times this transaction occurs in the database.
        /// </summary>
        /// <value>The number of times this transaction occurs in the database.</value>
        int Count { get; }
    }
}
