// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Provides methods to allow output to be committed.
    /// </summary>
    public interface IOutputCommitter
    {
        /// <summary>
        /// Gets the record writer for this output.
        /// </summary>
        /// <value>
        /// The record writer for this output.
        /// </value>
        IRecordWriter RecordWriter { get; }

        /// <summary>
        /// Commits the output.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        /// <remarks>
        /// The <see cref="RecordWriter"/> will be disposed before this method is called.
        /// </remarks>
        void Commit(FileSystemClient fileSystem);
    }
}
