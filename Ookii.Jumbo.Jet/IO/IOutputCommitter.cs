// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs.FileSystem;

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
