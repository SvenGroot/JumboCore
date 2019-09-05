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
    /// Committer for file output.
    /// </summary>
    public sealed class FileOutputCommitter : IOutputCommitter
    {
        private readonly IRecordWriter _recordWriter;
        private readonly string _tempFileName;
        private readonly string _outputFileName;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileOutputCommitter"/> class.
        /// </summary>
        /// <param name="recordWriter">The record writer.</param>
        /// <param name="tempFileName">Name of the temporary file that the data is written to.</param>
        /// <param name="outputFileName">Name of the output file that the temporary file should be renamed to.</param>
        public FileOutputCommitter(IRecordWriter recordWriter, string tempFileName, string outputFileName)
        {
            if( recordWriter == null )
                throw new ArgumentNullException("recordWriter");
            if( tempFileName == null )
                throw new ArgumentNullException("tempFileName");
            if( outputFileName == null )
                throw new ArgumentNullException("outputFileName");

            _recordWriter = recordWriter;
            _tempFileName = tempFileName;
            _outputFileName = outputFileName;
        }

        /// <summary>
        /// Gets the record writer for this output.
        /// </summary>
        /// <value>
        /// The record writer for this output.
        /// </value>
        public IRecordWriter RecordWriter
        {
            get { return _recordWriter; }
        }

        /// <summary>
        /// Commits the output.
        /// </summary>
        /// <param name="fileSystem">The file system.</param>
        public void Commit(FileSystemClient fileSystem)
        {
            if( fileSystem == null )
                throw new ArgumentNullException("fileSystem");

            fileSystem.Move(_tempFileName, _outputFileName);
        }
    }
}
