// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Interface for task classes.
    /// </summary>
    /// <typeparam name="TInput">The input type of the task.</typeparam>
    /// <typeparam name="TOutput">The output type of the task.</typeparam>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface ITask<TInput, TOutput>
    {
        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
        /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
        void Run(RecordReader<TInput> input, RecordWriter<TOutput> output);
    }
}
