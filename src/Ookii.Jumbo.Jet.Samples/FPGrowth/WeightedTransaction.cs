// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using System.Globalization;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    /// <summary>
    /// Transaction with count.
    /// </summary>
    public class WeightedTransaction : ITransaction
    {
        /// <summary>
        /// Gets or sets the count.
        /// </summary>
        /// <value>The count.</value>
        public int Count { get; set; }
        /// <summary>
        /// Gets or sets the items.
        /// </summary>
        /// <value>The items.</value>
        public int[] Items { get; set; }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}:{1}", Items.ToDelimitedString(), Count);
        }

        IEnumerable<int> ITransaction.Items
        {
            get { return Items; }
        }
    }
}
