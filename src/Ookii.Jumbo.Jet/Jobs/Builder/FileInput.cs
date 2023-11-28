// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Represents input read from the DFS for a job being constructed by the <see cref="JobBuilder"/> class.
/// </summary>
public sealed class FileInput : IOperationInput
{
    private readonly string _path;
    private readonly Type _recordReaderType;
    private readonly Type? _recordType;

    internal FileInput(string path, Type recordReaderType)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(recordReaderType);
        if (recordReaderType.ContainsGenericParameters)
        {
            throw new ArgumentException("The record reader type must be a closed constructed generic type.", nameof(recordReaderType));
        }

        var recordReaderBaseType = recordReaderType.FindGenericBaseType(typeof(RecordReader<>), true)!;

        _path = path;
        _recordReaderType = recordReaderType;
        if (!_recordReaderType.IsGenericTypeDefinition)
        {
            _recordType = recordReaderBaseType.GetGenericArguments()[0];
        }

        MinimumSplitSize = 1;
        MaximumSplitSize = Int32.MaxValue;
    }

    /// <summary>
    /// Gets the path of a directory or file on the DFS that the input will be read from.
    /// </summary>
    /// <value>The path of a directory or file on the DFS.</value>
    public string Path
    {
        get { return _path; }
    }

    /// <summary>
    /// Gets the type of the record reader.
    /// </summary>
    /// <value>The <see cref="Type"/> instance for the record reader. This is a class inheriting from <see cref="RecordReader{T}"/> where T is <see cref="RecordType"/>.</value>
    public Type RecordReaderType
    {
        get { return _recordReaderType; }
    }

    /// <summary>
    /// Gets the type of the records read from the input.
    /// </summary>
    /// <value>
    /// A <see cref="Type"/> instance for the type of the records.
    /// </value>
    public Type? RecordType
    {
        get { return _recordType; }
    }

    /// <summary>
    /// Gets or sets the minimum split size used to divide this input over multiple tasks.
    /// </summary>
    /// <value>
    /// The minimum split size. The default value is 1.
    /// </value>
    public int MinimumSplitSize { get; set; }

    /// <summary>
    /// Gets or sets the maximum split size used to divide this input over multiple tasks.
    /// </summary>
    /// <value>
    /// The maximum split size. The default value is <see cref="Int32.MaxValue"/>.
    /// </value>
    public int MaximumSplitSize { get; set; }

    /// <summary>
    /// Creates an <see cref="IDataInput"/> for this input.
    /// </summary>
    /// <param name="fileSystem">The file system.</param>
    /// <returns>The <see cref="IDataInput"/>.</returns>
    public IO.IDataInput CreateStageInput(FileSystemClient fileSystem)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        return new FileDataInput(fileSystem.Configuration, RecordReaderType, fileSystem.GetFileSystemEntryInfo(Path)!, MinimumSplitSize, MaximumSplitSize);
    }
}
