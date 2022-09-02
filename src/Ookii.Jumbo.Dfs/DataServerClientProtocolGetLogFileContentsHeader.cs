// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Header sent to the data server for the GetLogFileContents command.
    /// </summary>
    [Serializable]
    public class DataServerClientProtocolGetLogFileContentsHeader : DataServerClientProtocolHeader
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataServerClientProtocolGetLogFileContentsHeader"/> class with
        /// the specified maximum size.
        /// </summary>
        /// <param name="maxSize">The maximum size of the log data to return.</param>
        public DataServerClientProtocolGetLogFileContentsHeader(int maxSize)
            : base(DataServerCommand.GetLogFileContents)
        {
            MaxSize = maxSize;
        }

        /// <summary>
        /// Gets the maximum size of the log data to return.
        /// </summary>
        /// <value>
        /// The maximum size of the log data to return, in bytes.
        /// </value>
        public int MaxSize { get; private set; }

        /// <summary>
        /// Gets or sets the type of the log file that the data server return.
        /// </summary>
        /// <value>The type of the log file.</value>
        /// <remarks>
        /// <para>
        ///   When returning standard error and standard output stream, it's assumed the files containing these are in the directory specified by <see cref="Ookii.Jumbo.LogConfigurationElement.Directory"/>.
        /// </para>
        /// </remarks>
        public LogFileKind Kind { get; set; }
    }
}
