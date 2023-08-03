// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Generates a key/value pair for each record in the input where the value is an <see cref="Int32"/>.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   The value assigned to each key/value pair is 1 by default, but can be overridden by specifying a setting
    ///   with the <see cref="TaskConstants.GeneratePairTaskDefaultValueKey"/> in the <see cref="Jobs.StageConfiguration.StageSettings"/>.
    /// </para>
    /// </remarks>
    [AllowRecordReuse(PassThrough = true)]
    public sealed class GenerateInt32PairTask<T> : Configurable, ITask<T, Pair<T, int>>
        where T : notnull, IComparable<T>
    {
        private int _value = 1;

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<T>? input, RecordWriter<Pair<T, int>> output)
        {
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(output);
            if (TaskContext != null && TaskContext.StageConfiguration.AllowOutputRecordReuse)
            {
                // Record reuse allowed
                var result = new Pair<T, int>(default(T), _value);
                while (input.ReadRecord())
                {
                    result.Key = input.CurrentRecord;
                    output.WriteRecord(result);
                }
            }
            else
            {
                // Record reuse not allowed
                while (input.ReadRecord())
                {
                    output.WriteRecord(new Pair<T, int>(input.CurrentRecord, _value));
                }
            }
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public override void NotifyConfigurationChanged()
        {
            if (TaskContext != null)
            {
                _value = TaskContext.StageConfiguration.GetSetting(TaskConstants.GeneratePairTaskDefaultValueKey, 1);
            }
        }
    }
}
