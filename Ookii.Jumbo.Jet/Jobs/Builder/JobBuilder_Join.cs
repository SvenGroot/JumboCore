// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Performs an inner equi-join on two inputs.
        /// </summary>
        /// <param name="outerInput">The outer input for the join operation.</param>
        /// <param name="innerInput">The inner input for the join operation.</param>
        /// <param name="innerJoinRecordReaderType">Type of the inner join record reader.</param>
        /// <param name="outerComparerType">Type of the comparer used to sort the outer relation. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> are supported, but using <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> is strongly recommended.</param>
        /// <param name="innerComparerType">Type of the comparer used to sort the inner relation. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> are supported, but using <see cref="Ookii.Jumbo.IO.IRawComparer{T}"/> is strongly recommended.</param>
        /// <remarks>
        /// <para>
        ///   This operation joins two inputs by first sorting them (by using a spill sort on the input channel for each input) and
        ///   then joining them using an inner equi-join using the <see cref="Ookii.Jumbo.IO.InnerJoinRecordReader{TOuter,TInner,TResult}"/>.
        /// </para>
        /// <para>
        ///   In order for the join to be performed correctly, both the outer and inner input must be sorted on the join attribute.
        ///   Please set <paramref name="outerComparerType"/> and <paramref name="innerComparerType"/> accordingly.
        /// </para>
        /// <para>
        ///   If the join uses more than one task, both the outer and inner input must be partitioned on the join attribute. For
        ///   this purpose, the outerComparerType or innerComparerType should also implement <see cref="IEqualityComparer{T}"/>,
        ///   or you should manually set a different <see cref="Channel.PartitionerType"/>.
        /// </para>
        /// </remarks>
        public InnerJoinOperation InnerJoin(IOperationInput outerInput, IOperationInput innerInput, Type innerJoinRecordReaderType, Type outerComparerType, Type innerComparerType)
        {
            if( outerInput == null )
                throw new ArgumentNullException("outerInput");
            if( innerInput == null )
                throw new ArgumentNullException("innerInput");
            if( innerJoinRecordReaderType == null )
                throw new ArgumentNullException("innerJoinRecordReaderType");

            CheckIfInputBelongsToJobBuilder(outerInput);
            CheckIfInputBelongsToJobBuilder(innerInput);

            return new InnerJoinOperation(this, outerInput, innerInput, innerJoinRecordReaderType, outerComparerType, innerComparerType);
        }
    }
}
