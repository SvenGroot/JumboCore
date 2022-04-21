// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Tasks
{
    /// <summary>
    /// A task that does nothing, but simply forwards the records to the output unmodified.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// This task is useful if you immediately want to partition your input without processing it first.
    /// </remarks>
    [AllowRecordReuse(PassThrough = true)]
    public class EmptyTask<T> : ITask<T, T>
    {
        /// <summary>
        /// Runs the task.
        /// </summary>
        /// <param name="input">The input for the task.</param>
        /// <param name="output">The output for the task.</param>
        public void Run(RecordReader<T> input, RecordWriter<T> output)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));
            if (output == null)
                throw new ArgumentNullException(nameof(output));
            foreach (T record in input.EnumerateRecords())
            {
                output.WriteRecord(record);
            }
        }
    }
}
