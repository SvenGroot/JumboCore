// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Abstract base class for reduce tasks.
    /// </summary>
    /// <typeparam name="TKey">The type of the key of the input records.</typeparam>
    /// <typeparam name="TValue">The type of the value of the input records.</typeparam>
    /// <typeparam name="TOutput">The type of the output records.</typeparam>
    /// <remarks>
    /// <note>
    ///   This task requires that the input is sorted by key.
    /// </note>
    /// <para>
    ///   Because the <see cref="ReduceTask{TKey,TValue,TOutput}"/> class is a pull task, it is not recommended to use
    ///   it as the receiving stage of an in-process channel.
    /// </para>
    /// <para>
    ///   If the reduce function could be calculated incrementally, <see cref="AccumulatorTask{TKey,TValue}"/> often offers
    ///   much better performance than sorting and using a reduce task.
    /// </para>
    /// <para>
    ///   You may specify the <see cref="Ookii.Jumbo.Jet.AllowRecordReuseAttribute"/> attribute on classes deriving from this type
    ///   only if <typeparamref name="TKey"/> is a value type or implements <see cref="ICloneable"/>.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1005:AvoidExcessiveParametersOnGenericTypes")]
    public abstract class ReduceTask<TKey, TValue, TOutput> : Configurable, ITask<Pair<TKey, TValue>, TOutput>
        where TKey : IComparable<TKey>
    {
        private IEqualityComparer<TKey> _keyComparer;
        private readonly bool _cloneKey;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReduceTask&lt;TKey, TValue, TOutput&gt;"/> class.
        /// </summary>
        protected ReduceTask()
        {
            if( Attribute.IsDefined(GetType(), typeof(AllowRecordReuseAttribute)) )
            {
                _cloneKey = !typeof(TKey).IsValueType;
            }
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<Pair<TKey, TValue>> input, RecordWriter<TOutput> output)
        {
            if( input != null && input.ReadRecord() )
            {
                do
                {
                    TKey key = _cloneKey ? (TKey)((ICloneable)input.CurrentRecord.Key).Clone() : input.CurrentRecord.Key;
                    Reduce(key, EnumerateGroupRecords(key, input), output);
                } while( !input.HasFinished );
            }
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            if( TaskContext != null )
            {
                string comparerTypeName = TaskContext.StageConfiguration.GetSetting(TaskConstants.ReduceTaskKeyComparerSettingKey, null);
                if( !string.IsNullOrEmpty(comparerTypeName) )
                    _keyComparer = (IEqualityComparer<TKey>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskContext);
            }

            if( _keyComparer == null )
                _keyComparer = EqualityComparer<TKey>.Default;
        }

        /// <summary>
        /// Reduces the values for the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="values">The values associated with the key.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        protected abstract void Reduce(TKey key, IEnumerable<TValue> values, RecordWriter<TOutput> output);

        private IEnumerable<TValue> EnumerateGroupRecords(TKey key, RecordReader<Pair<TKey, TValue>> input)
        {
            // Checking HasFinished and comparing the first key may seem pointless, but it guards against a reducer trying to use the iterator twice.
            while( !input.HasFinished && _keyComparer.Equals(key, input.CurrentRecord.Key) )
            {
                yield return input.CurrentRecord.Value;
                input.ReadRecord();
            }
        }
    }
}
