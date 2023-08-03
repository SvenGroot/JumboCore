using System;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Represents the output of an operation. Can either be a channel or DFS output.
    /// </summary>
    public interface IOperationOutput
    {
        /// <summary>
        /// Gets the type of the records that can be written to this output.
        /// </summary>
        /// <value>
        /// The type of the records.
        /// </value>
        Type? RecordType { get; }

        /// <summary>
        /// Applies the output settings to the specified stage.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <param name="stage">The stage.</param>
        /// <remarks>
        /// This does nothing for channels; it is only relevant for DFS output.
        /// </remarks>
        void ApplyOutput(FileSystemClient fileSystem, StageConfiguration stage);
    }
}
