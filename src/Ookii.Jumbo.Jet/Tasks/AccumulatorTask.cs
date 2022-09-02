// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Base class for tasks that accumulate values associated with a specific key.
    /// </summary>
    /// <typeparam name="TKey">The type of the keys.</typeparam>
    /// <typeparam name="TValue">The type of the values.</typeparam>
    /// <remarks>
    /// <para>
    ///   It is safe to reuse the same <see cref="Pair{TKey,TValue}"/> in every call to
    ///   <see cref="ProcessRecord"/> if the key and value are either value types or 
    ///   implement <see cref="ICloneable"/>. Therefore, if you specify the <see cref="AllowRecordReuseAttribute"/> on a class
    ///   deriving from this class, the key and value must either be value types or
    ///   implement <see cref="ICloneable"/>.
    /// </para>
    /// <para>
    ///   You can specify a custom key comparer using the <see cref="TaskConstants.AccumulatorTaskKeyComparerSettingKey"/> key
    ///   in the stage settings. Note that it is recommended to also use that has the comparer type for the <see cref="HashPartitioner{T}"/> in that case.
    /// </para>
    /// </remarks>
    public abstract class AccumulatorTask<TKey, TValue> : PushTask<Pair<TKey, TValue>, Pair<TKey, TValue>>
        where TKey : IComparable<TKey>
    {
        #region Nested types

        private sealed class ValueContainer
        {
            public TValue Value { get; set; }
        }

        #endregion

        private Dictionary<TKey, ValueContainer> _acculumatedValues;

        private readonly bool _cloneKey;
        private readonly bool _cloneValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="AccumulatorTask{TKey,TValue}"/> class.
        /// </summary>
        protected AccumulatorTask()
        {
            if (Attribute.IsDefined(GetType(), typeof(AllowRecordReuseAttribute)))
            {
                _cloneKey = !typeof(TKey).IsValueType;
                _cloneValue = !typeof(TValue).IsValueType;
            }
        }

        /// <summary>
        /// Method called for each record in the task's input.
        /// </summary>
        /// <param name="record">The record to process.</param>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public override void ProcessRecord(Pair<TKey, TValue> record, RecordWriter<Pair<TKey, TValue>> output)
        {
            if (_acculumatedValues == null)
                _acculumatedValues = new Dictionary<TKey, ValueContainer>();

            if (_acculumatedValues.TryGetValue(record.Key, out var value))
                value.Value = Accumulate(record.Key, value.Value, record.Value);
            else
            {
                TKey key;
                if (_cloneKey)
                    key = (TKey)((ICloneable)record.Key).Clone();
                else
                    key = record.Key;

                value = new ValueContainer();
                if (_cloneValue)
                    value.Value = (TValue)((ICloneable)record.Value).Clone();
                else
                    value.Value = record.Value;

                _acculumatedValues.Add(key, value);
            }
        }

        /// <summary>
        /// Method called after the last record was processed.
        /// </summary>
        /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        /// <remarks>
        /// This enables the task to finish up its processing and write any further records it may have collected during processing.
        /// </remarks>
        public override void Finish(RecordWriter<Pair<TKey, TValue>> output)
        {
            ArgumentNullException.ThrowIfNull(output);
            var allowRecordReuse = TaskContext.StageConfiguration.AllowOutputRecordReuse;
            Pair<TKey, TValue> record = null;
            if (allowRecordReuse)
                record = new Pair<TKey, TValue>();
            foreach (var item in _acculumatedValues)
            {
                if (!allowRecordReuse)
                    record = new Pair<TKey, TValue>();
                record.Key = item.Key;
                record.Value = item.Value.Value;
                output.WriteRecord(record);
            }
        }

        /// <summary>
        /// When implemented in a derived class, accumulates the values of the records.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        /// <param name="currentValue">The current value associated with the key.</param>
        /// <param name="newValue">The new value associated with the key.</param>
        /// <returns>The new value.</returns>
        /// <remarks>
        /// <para>
        ///   If <typeparamref name="TValue"/> is a mutable reference type, it is recommended for performance reasons to update the
        ///   existing instance passed in <paramref name="currentValue"/> and then return that same instance from this method.
        /// </para>
        /// </remarks>
        protected abstract TValue Accumulate(TKey key, TValue currentValue, TValue newValue);

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration" /> calls this method
        /// after setting the configuration.
        /// </summary>
        /// <exception cref="System.InvalidOperationException">Cannot change configuration after accumulation has started.</exception>
        public override void NotifyConfigurationChanged()
        {
            base.NotifyConfigurationChanged();
            if (TaskContext != null)
            {
                if (_acculumatedValues != null && _acculumatedValues.Count > 0)
                    throw new InvalidOperationException("Cannot change configuration after accumulation has started.");

                var comparerTypeName = TaskContext.StageConfiguration.GetSetting(TaskConstants.AccumulatorTaskKeyComparerSettingKey, null);
                IEqualityComparer<TKey> comparer = null;
                if (comparerTypeName != null)
                {
                    var comparerType = Type.GetType(comparerTypeName, true);
                    comparer = (IEqualityComparer<TKey>)JetActivator.CreateInstance(comparerType, DfsConfiguration, JetConfiguration, TaskContext);
                }
                _acculumatedValues = new Dictionary<TKey, ValueContainer>(comparer);
            }
        }
    }
}
