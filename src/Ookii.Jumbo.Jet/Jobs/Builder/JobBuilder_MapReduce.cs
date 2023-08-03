// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Runs a map function on each record in the input.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="mapper">The map function.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <note>
        ///   There is no non-delegate version of this method. To use an existing map task class, simply use the <see cref="Process(IOperationInput,Type)"/> function.
        /// </note>
        /// <para>
        ///   This method generates a class inheriting from <see cref="PushTask{TInput,TOutput}"/> which calls the target method of the <paramref name="mapper"/> delegate
        ///   from the <see cref="PushTask{TInput,TOutput}.ProcessRecord"/> method.
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
        public StageOperation Map<TInput, TOutput>(IOperationInput input, Action<TInput, RecordWriter<TOutput>, TaskContext> mapper, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TInput : notnull
            where TOutput : notnull
        {
            return MapCore<TInput, TOutput>(input, mapper, recordReuse);
        }

        /// <summary>
        /// Runs a map function on each record in the input.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="mapper">The map function.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <note>
        ///   There is no non-delegate version of this method. To use an existing map task class, simply use the <see cref="Process(IOperationInput,Type)"/> function.
        /// </note>
        /// <para>
        ///   This method generates a class inheriting from <see cref="PushTask{TInput,TOutput}"/> which calls the target method of the <paramref name="mapper"/> delegate
        ///   from the <see cref="PushTask{TInput,TOutput}.ProcessRecord"/> method.
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
        public StageOperation Map<TInput, TOutput>(IOperationInput input, Action<TInput, RecordWriter<TOutput>> mapper, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TInput : notnull
            where TOutput : notnull
        {
            return MapCore<TInput, TOutput>(input, mapper, recordReuse);
        }

        /// <summary>
        /// Runs a reduce function on each key in the specified input.
        /// </summary>
        /// <typeparam name="TKey">The type of the keys.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="reducer">The reducer function.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <note>
        ///   Reduce tasks require that their input is already grouped by key. The <see cref="JobBuilder"/> class doesn't guarantee or verify this. To group the
        ///   records in the same way other common MapReduce implementation do, use the <see cref="SpillSortCombine"/> function.
        /// </note>
        /// <note>
        ///   There is no non-delegate version of this method. To use an existing map task class, simply use the <see cref="Process(IOperationInput,Type)"/> function.
        /// </note>
        /// <para>
        ///   This method generates a class inheriting from <see cref="ReduceTask{TKey,TValue,TOutput}"/> which calls the target method of the <paramref name="reducer"/> delegate
        ///   from the <see cref="ReduceTask{TKey,TValue,TOutput}.Reduce"/> method.
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
        public StageOperation Reduce<TKey, TValue, TOutput>(IOperationInput input, Action<TKey, IEnumerable<TValue>, RecordWriter<TOutput>, TaskContext> reducer, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : notnull, IComparable<TKey>
            where TValue : notnull
            where TOutput : notnull
        {
            return ReduceCore<TKey, TValue, TOutput>(input, reducer, recordReuse);
        }

        /// <summary>
        /// Runs a reduce function on each key in the specified input.
        /// </summary>
        /// <typeparam name="TKey">The type of the keys.</typeparam>
        /// <typeparam name="TValue">The type of the values.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="reducer">The reducer function.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="StageOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <note>
        ///   Reduce tasks require that their input is already grouped by key. The <see cref="JobBuilder"/> class doesn't guarantee or verify this. To group the
        ///   records in the same way other common MapReduce implementation do, use the <see cref="SpillSortCombine"/> function.
        /// </note>
        /// <note>
        ///   There is no non-delegate version of this method. To use an existing map task class, simply use the <see cref="Process(IOperationInput,Type)"/> function.
        /// </note>
        /// <para>
        ///   This method generates a class inheriting from <see cref="ReduceTask{TKey,TValue,TOutput}"/> which calls the target method of the <paramref name="reducer"/> delegate
        ///   from the <see cref="ReduceTask{TKey,TValue,TOutput}.Reduce"/> method.
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
        public StageOperation Reduce<TKey, TValue, TOutput>(IOperationInput input, Action<TKey, IEnumerable<TValue>, RecordWriter<TOutput>> reducer, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : notnull, IComparable<TKey>
            where TValue : notnull
            where TOutput : notnull
        {
            return ReduceCore<TKey, TValue, TOutput>(input, reducer, recordReuse);
        }

        private StageOperation MapCore<TInput, TOutput>(IOperationInput input, Delegate mapper, RecordReuseMode recordReuse)
            where TInput : notnull
            where TOutput : notnull
        {
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(mapper);
            CheckIfInputBelongsToJobBuilder(input);

            var taskType = _taskBuilder.CreateDynamicTask(typeof(PushTask<TInput, TOutput>).GetMethod("ProcessRecord")!, mapper, 0, recordReuse);

            var result = new StageOperation(this, input, taskType);
            AddAssemblyAndSerializeDelegateIfNeeded(mapper, result);
            return result;
        }

        private StageOperation ReduceCore<TKey, TValue, TOutput>(IOperationInput input, Delegate reducer, RecordReuseMode recordReuse)
            where TKey : notnull, IComparable<TKey>
            where TValue : notnull
            where TOutput : notnull
        {
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(reducer);
            CheckIfInputBelongsToJobBuilder(input);

            var taskType = CreateReduceTask<TKey, TValue, TOutput>(reducer, recordReuse);

            var result = new StageOperation(this, input, taskType);
            AddAssemblyAndSerializeDelegateIfNeeded(reducer, result);
            return result;
        }

        private Type CreateReduceTask<TKey, TValue, TOutput>(Delegate reducer, RecordReuseMode recordReuse)
            where TKey : notnull, IComparable<TKey>
            where TValue : notnull
            where TOutput : notnull
        {
            var taskType = _taskBuilder.CreateDynamicTask(typeof(ReduceTask<TKey, TValue, TOutput>).GetMethod("Reduce", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!, reducer, 0, recordReuse);
            return taskType;
        }
    }
}
