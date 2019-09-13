// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Task that computes the sum of the value of each key in the input data.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <remarks>
    /// <para>
    ///   Because this class has the <see cref="AllowRecordReuseAttribute"/> set,
    ///   <typeparamref name="TKey"/> must be either a value type or implement
    ///   <see cref="ICloneable"/>.
    /// </para>
    /// </remarks>
    [AllowRecordReuse]
    public sealed class SumTask<TKey> : AccumulatorTask<TKey, int>
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Updates the sum for the specified key.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        /// <param name="currentValue">The current value associated with the key.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        /// <returns>The updated value, which is the sum of the current value and the new value.</returns>
        protected override int Accumulate(TKey key, int currentValue, int newValue)
        {
            return currentValue + newValue;
        }
    }
}
