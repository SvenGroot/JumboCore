// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Reflection;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    public sealed partial class JobBuilder
    {
        /// <summary>
        /// Groups the input records by key, then aggregates their values.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="accumulatorTaskType">The type of the accumulator task used to collect the aggregated values. May be a generic type definition with one or two generic parameters.</param>
        /// <param name="keyComparerType">Type of the <see cref="IEqualityComparer{T}"/> to use to compare the aggregation keys, 
        /// or <see langword="null" /> to use <see cref="EqualityComparer{T}.Default"/>. May be a generic type definition with one type parameter, which will be set to the key type.</param>
        /// <returns>A <see cref="TwoStepOperation"/> instance that can be used to further customize the operation.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1062:Validate arguments of public methods", Justification = "False positive.")]
        public TwoStepOperation GroupAggregate(IOperationInput input, Type accumulatorTaskType, Type? keyComparerType = null)
        {
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(accumulatorTaskType);

            if (accumulatorTaskType.IsGenericTypeDefinition)
            {
                if (!(input.RecordType!.IsGenericType && input.RecordType.GetGenericTypeDefinition() == typeof(Pair<,>)))
                    throw new ArgumentException("The input record type must be Pair<TKey,TValue> for group aggregation.", nameof(input));

                accumulatorTaskType = ConstructGenericAccumulatorTaskType(input.RecordType, accumulatorTaskType);
            }

            var taskBaseType = accumulatorTaskType.FindGenericBaseType(typeof(AccumulatorTask<,>), true)!; // Ensure it's an accumulator.

            if (keyComparerType != null)
            {
                if (keyComparerType.IsGenericTypeDefinition)
                    keyComparerType = keyComparerType.MakeGenericType(taskBaseType.GetGenericArguments()[0]);

                var comparerBaseType = keyComparerType.FindGenericInterfaceType(typeof(IEqualityComparer<>), true)!;
                if (comparerBaseType.GetGenericArguments()[0] != taskBaseType.GetGenericArguments()[0])
                    throw new ArgumentException("Comparer type is not an IEqualityComparer for the key type.", nameof(keyComparerType));
            }

            CheckIfInputBelongsToJobBuilder(input);
            var result = new TwoStepOperation(this, input, accumulatorTaskType, null, false);

            if (keyComparerType != null)
            {
                AddAssembly(accumulatorTaskType.Assembly);
                result.Settings.Add(TaskConstants.AccumulatorTaskKeyComparerSettingKey, keyComparerType.AssemblyQualifiedName!);
                result.InputChannel!.Settings.Add(PartitionerConstants.EqualityComparerSetting, keyComparerType.AssemblyQualifiedName!);
            }

            return result;
        }

        /// <summary>
        /// Groups the input records by key, then aggregates their values using the specified accumulator task function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="accumulator">The accumulator function to use to create the task.</param>
        /// <param name="keyComparerType">Type of the <see cref="IEqualityComparer{T}"/> to use to compare the aggregation keys, 
        /// or <see langword="null" /> to use <see cref="EqualityComparer{T}.Default"/>. May be a generic type definition with one type parameter, which will be set to the key type.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="TwoStepOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="AccumulatorTask{TKey,TValue}"/> which calls the target method of the <paramref name="accumulator"/> delegate
        ///   from the <see cref="AccumulatorTask{TKey,TValue}.Accumulate"/> method.
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
        public TwoStepOperation GroupAggregate<TKey, TValue>(IOperationInput input, Func<TKey, TValue, TValue, TaskContext, TValue> accumulator, Type? keyComparerType = null, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : notnull, IComparable<TKey> // Requirement for AccumulatorTask
            where TValue : notnull
        {
            return GroupAggregateCore<TKey, TValue>(input, accumulator, keyComparerType, recordReuse);
        }

        /// <summary>
        /// Groups the input records by key, then aggregates their values using the specified accumulator task function.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="accumulator">The accumulator function to use to create the task.</param>
        /// <param name="keyComparerType">Type of the <see cref="IEqualityComparer{T}"/> to use to compare the aggregation keys, 
        /// or <see langword="null" /> to use <see cref="EqualityComparer{T}.Default"/>. May be a generic type definition with one type parameter, which will be set to the key type.</param>
        /// <param name="recordReuse">The record reuse mode.</param>
        /// <returns>A <see cref="TwoStepOperation"/> instance that can be used to further customize the operation.</returns>
        /// <remarks>
        /// <para>
        ///   This method generates a class implementing <see cref="AccumulatorTask{TKey,TValue}"/> which calls the target method of the <paramref name="accumulator"/> delegate
        ///   from the <see cref="AccumulatorTask{TKey,TValue}.Accumulate"/> method.
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
        public TwoStepOperation GroupAggregate<TKey, TValue>(IOperationInput input, Func<TKey, TValue, TValue, TValue> accumulator, Type? keyComparerType = null, RecordReuseMode recordReuse = RecordReuseMode.Default)
            where TKey : notnull, IComparable<TKey> // Requirement for AccumulatorTask
            where TValue : notnull
        {
            return GroupAggregateCore<TKey, TValue>(input, accumulator, keyComparerType, recordReuse);
        }

        private TwoStepOperation GroupAggregateCore<TKey, TValue>(IOperationInput input, Delegate accumulator, Type? keyComparerType, RecordReuseMode recordReuse)
            where TKey : notnull, IComparable<TKey> // Requirement for AccumulatorTask
            where TValue : notnull
        {
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(accumulator);
            CheckIfInputBelongsToJobBuilder(input);

            var taskType = _taskBuilder.CreateDynamicTask(typeof(AccumulatorTask<TKey, TValue>).GetMethod("Accumulate", BindingFlags.NonPublic | BindingFlags.Instance)!, accumulator, 0, recordReuse);

            var result = GroupAggregate(input, taskType, keyComparerType);
            AddAssemblyAndSerializeDelegateIfNeeded(accumulator, result);
            return result;
        }

        private static Type ConstructGenericAccumulatorTaskType(Type recordType, Type accumulatorTaskType)
        {
            Type[] arguments;
            var parameters = accumulatorTaskType.GetGenericArguments();
            switch (parameters.Length)
            {
            case 1:
                switch (parameters[0].Name)
                {
                case "TKey":
                    arguments = new[] { recordType.GetGenericArguments()[0] };
                    break;
                case "TValue":
                    arguments = new[] { recordType.GetGenericArguments()[1] };
                    break;
                default:
                    throw new ArgumentException("Could not determine whether to use the key or value type to construct the generic type.", nameof(accumulatorTaskType));
                }
                break;
            case 2:
                // We assume the two parameters are TKey and TValue
                arguments = recordType.GetGenericArguments();
                break;
            default:
                throw new ArgumentException("The accumulator type has an unsupported number of generic type parameters.", nameof(accumulatorTaskType));
            }
            accumulatorTaskType = accumulatorTaskType.MakeGenericType(arguments);
            return accumulatorTaskType;
        }
    }
}
