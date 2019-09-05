// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides methods for defining input (other than a channel) to a stage.
    /// </summary>
    public interface IDataInput
    {
        /// <summary>
        /// Gets the inputs for each task.
        /// </summary>
        /// <value>
        /// A list of task inputs, or <see langword="null"/> if the job is not being constructed. The returned collection may be read-only.
        /// </value>
        IList<ITaskInput> TaskInputs { get; }

        /// <summary>
        /// Gets the type of the records of this input.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        Type RecordType { get; }

        /// <summary>
        /// Creates the record reader for the specified task.
        /// </summary>
        /// <param name="input">The task input.</param>
        /// <returns>
        /// The record reader.
        /// </returns>
        IRecordReader CreateRecordReader(ITaskInput input);

        /// <summary>
        /// Notifies the data input that it has been added to a stage.
        /// </summary>
        /// <param name="stage">The stage configuration of the stage.</param>
        /// <remarks>
        /// <para>
        ///   Implement this method if you want to add any setting to the stage. Keep in mind that the stage may still be under construction, so not all its
        ///   properties may have their final values yet.
        /// </para>
        /// </remarks>
        void NotifyAddedToStage(StageConfiguration stage);
    }
}
