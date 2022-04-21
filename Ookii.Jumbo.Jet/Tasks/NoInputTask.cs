// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Provides a more convenient interface for tasks that have no input.
    /// </summary>
    /// <typeparam name="T">The type of the output records.</typeparam>
    [AdditionalProgressCounter("Task")]
    public abstract class NoInputTask<T> : Configurable, ITask<int, T>, IHasAdditionalProgress
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NoInputTask<>));

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        public void Run(RecordReader<int> input, RecordWriter<T> output)
        {
            if (input != null)
                _log.Warn("Input was provided but will be ignored by this task.");
            Run(output);
        }

        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        protected abstract void Run(RecordWriter<T> output);

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>
        /// The additional progress value.
        /// </value>
        public float AdditionalProgress { get; protected set; }
    }
}
