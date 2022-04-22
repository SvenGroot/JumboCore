// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// Provides a method for task functions used with the <see cref="Ookii.Jumbo.Jet.Jobs.Builder.JobBuilder.Generate{T}(int, Action{Ookii.Jumbo.IO.RecordWriter{T},ProgressContext})"/> method
    /// to report progress.
    /// </summary>
    public sealed class ProgressContext
    {
        private readonly IConfigurable _configurable;

        internal ProgressContext(IConfigurable configurable)
        {
            if (configurable == null)
                throw new ArgumentNullException(nameof(configurable));
            _configurable = configurable;
        }

        /// <summary>
        /// Gets the task context for this task.
        /// </summary>
        /// <value>
        /// The <see cref="TaskContext"/> for this task.
        /// </value>
        public TaskContext TaskContext
        {
            get { return _configurable.TaskContext; }
        }

        /// <summary>
        /// Gets or sets the progress of task.
        /// </summary>
        /// <value>
        /// The progress of the task, between 0 and 1.
        /// </value>
        public float Progress { get; set; }
    }
}
