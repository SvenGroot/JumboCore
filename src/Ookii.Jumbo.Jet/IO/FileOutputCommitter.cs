﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.IO;

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
        ArgumentNullException.ThrowIfNull(recordWriter);
        ArgumentNullException.ThrowIfNull(tempFileName);
        ArgumentNullException.ThrowIfNull(outputFileName);

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
        ArgumentNullException.ThrowIfNull(fileSystem);

        fileSystem.Move(_tempFileName, _outputFileName);
    }
}
