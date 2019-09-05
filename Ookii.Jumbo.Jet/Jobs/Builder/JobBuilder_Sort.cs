﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Sorts the specified input in memory using <see cref="Ookii.Jumbo.Jet.Tasks.SortTask{T}"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="comparerType">The type of <see cref="IComparer{T}"/> to use for this operation, or <see langword="null"/> to use the default comparer.</param>
        /// <returns>A <see cref="SortOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This operation sorts all the records produced by a single task in memory. For large or unknown amounts of records, use <see cref="SpillSortCombine"/> instead.
        /// </para>
        /// </remarks>
        public SortOperation MemorySort(IOperationInput input, Type comparerType = null)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            CheckIfInputBelongsToJobBuilder(input);
            return SortOperation.CreateMemorySortOperation(this, input, comparerType);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill" />.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="comparerType">Type of the comparer to use. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        /// <returns>
        /// A <see cref="SortOperation" /> instance that can be used to further customize the operation.
        /// </returns>
        public SortOperation SpillSort(IOperationInput input, Type comparerType = null)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            CheckIfInputBelongsToJobBuilder(input);
            return SortOperation.CreateSpillSortOperation(this, input, comparerType, null);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="combinerType">Type of the combiner task. May be <see langword="null"/>. May be a generic type definition with a single type parameter.</param>
        /// <param name="comparerType">Type of the comparer to use. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        /// <returns>
        /// A <see cref="SortOperation"/> instance that can be used to further customize the operation.
        /// </returns>
        public SortOperation SpillSortCombine(IOperationInput input, Type combinerType, Type comparerType = null)
        {
            if( input == null )
                throw new ArgumentNullException("input");
            CheckIfInputBelongsToJobBuilder(input);
            return SortOperation.CreateSpillSortOperation(this, input, comparerType, combinerType);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill"/> and using
        /// the specified reduce-style combiner function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="combiner">The combiner function.</param>
        /// <param name="comparerType">Type of the comparer to use. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="SortOperation"/> instance that can be used to further customize the operation.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class inheriting from <see cref="Tasks.ReduceTask{TKey,TValue,TOutput}"/> which calls the target method of the <paramref name="combiner"/> delegate
        ///   from the <see cref="Tasks.ReduceTask{TKey,TValue,TOutput}.Reduce"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If <paramref name="recordReuse"/> is <see cref="RecordReuseMode.Default"/> and the target method has the <see cref="AllowRecordReuseAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class. If the target method has the <see cref="ProcessAllInputPartitionsAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class.
        /// </para>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public SortOperation SpillSortCombine<TKey, TValue>(IOperationInput input, Action<TKey, IEnumerable<TValue>, RecordWriter<Pair<TKey, TValue>>, TaskContext> combiner, Type comparerType = null, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : IComparable<TKey>
        {
            return SpillSortCombineCore<TKey, TValue>(input, combiner, comparerType, recordReuse);
        }

        /// <summary>
        /// Sorts the specified input by using a file channel with an output type of <see cref="Channels.FileChannelOutputType.SortSpill"/> and using
        /// the specified reduce-style combiner function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="combiner">The combiner function.</param>
        /// <param name="comparerType">Type of the comparer to use. May be <see langword="null"/>. May be a generic type definition with a single type parameter. Both <see cref="IComparer{T}"/> and <see cref="IRawComparer{T}"/> are supported, but using <see cref="IRawComparer{T}"/> is strongly recommended.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>
        /// A <see cref="SortOperation"/> instance that can be used to further customize the operation.
        /// </returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class inheriting from <see cref="Tasks.ReduceTask{TKey,TValue,TOutput}"/> which calls the target method of the <paramref name="combiner"/> delegate
        ///   from the <see cref="Tasks.ReduceTask{TKey,TValue,TOutput}.Reduce"/> method.
        /// </para>
        /// <note>
        ///   The task method will be called from a completely different process than the one that is using <see cref="JobBuilder"/>, so it should not really
        ///   on any external state.
        /// </note>
        /// <para>
        ///   If <paramref name="recordReuse"/> is <see cref="RecordReuseMode.Default"/> and the target method has the <see cref="AllowRecordReuseAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class. If the target method has the <see cref="ProcessAllInputPartitionsAttribute"/> attribute applied to it,
        ///   that attribute will be copied to the task class.
        /// </para>
        /// <para>
        ///   If the target method is a <see langword="public" /> <see langword="static"/> method, it will be called directly by the generated task class. Otherwise, the supplied
        ///   delegate will be serialized to the task settings and used to call the method. If the target method is an instance method, the instance it belongs to will be
        ///   serialized as well (this class must have the <see cref="SerializableAttribute"/> attribute).
        /// </para>
        /// </remarks>
        public SortOperation SpillSortCombine<TKey, TValue>(IOperationInput input, Action<TKey, IEnumerable<TValue>, RecordWriter<Pair<TKey, TValue>>> combiner, Type comparerType = null, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : IComparable<TKey>
        {
            return SpillSortCombineCore<TKey, TValue>(input, combiner, comparerType, recordReuse);
        }

        private SortOperation SpillSortCombineCore<TKey, TValue>(IOperationInput input, Delegate combiner, Type comparerType, RecordReuseMode recordReuse)
            where TKey : IComparable<TKey>
        {
            if( input == null )
                throw new ArgumentNullException("input");
            if( combiner == null )
                throw new ArgumentNullException("combiner");
            CheckIfInputBelongsToJobBuilder(input);

            Type combinerType = CreateReduceTask<TKey, TValue, Pair<TKey, TValue>>(combiner, recordReuse);
            
            SortOperation result = SpillSortCombine(input, combinerType, comparerType);
            AddAssemblyAndSerializeDelegateIfNeeded(combiner, result);
            return result;
        }
    }
}
