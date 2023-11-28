// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Base class for tasks that use the push model.
/// </summary>
/// <typeparam name="TInput">The input type of the task.</typeparam>
/// <typeparam name="TOutput">The output type of the task.</typeparam>
/// <remarks>
/// <para>
///   When a task inheriting from <see cref="PushTask{TInput, TOutput}"/> is used for the receiving stage of a
///   pipeline channel, the <see cref="Run"/> method will not be called.
/// </para>
/// </remarks>
public abstract class PushTask<TInput, TOutput> : Configurable, ITask<TInput, TOutput>
    where TInput : notnull
    where TOutput : notnull
{
    /// <summary>
    /// Method called for each record in the task's input.
    /// </summary>
    /// <param name="record">The record to process.</param>
    /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
    public abstract void ProcessRecord(TInput record, RecordWriter<TOutput> output);

    /// <summary>
    /// Method called after the last record was processed.
    /// </summary>
    /// <param name="output">The <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
    /// <remarks>
    /// This enables the task to finish up its processing and write any further records it may have collected during processing.
    /// </remarks>
    public virtual void Finish(RecordWriter<TOutput> output)
    {
    }

    /// <summary>
    /// Runs the task.
    /// </summary>
    /// <param name="input">A <see cref="RecordReader{T}"/> from which the task's input can be read.</param>
    /// <param name="output">A <see cref="RecordWriter{T}"/> to which the task's output should be written.</param>
    /// <remarks>
    /// <para>
    ///   While it is possible to override this method, it is not guaranteed to be called in all scenarios (e.g. if this
    ///   task is receiving data from a pipeline channel). The task must function correctly even if the <see cref="ProcessRecord"/>
    ///   and <see cref="Finish"/> are called directly.
    /// </para>
    /// </remarks>
    public virtual void Run(RecordReader<TInput>? input, RecordWriter<TOutput> output)
    {
        if (input != null)
        {
            foreach (var record in input.EnumerateRecords())
            {
                ProcessRecord(record, output);
            }
        }
        Finish(output);
    }
}
