using System;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Represents output written to the DFS for a job being constructed by the <see cref="JobBuilder"/> class.
/// </summary>
public sealed class FileOutput : IOperationOutput
{
    private readonly string _path;
    private readonly Type _recordWriterType;
    private readonly Type _recordType;

    internal FileOutput(string path, Type recordWriterType)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(recordWriterType);
        if (recordWriterType.ContainsGenericParameters)
        {
            throw new ArgumentException("The record writer type must be a closed constructed generic type.", nameof(recordWriterType));
        }

        _path = path;
        _recordWriterType = recordWriterType;
        var baseType = recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), true)!;
        _recordType = baseType.GetGenericArguments()[0];
    }

    /// <summary>
    /// Gets the path of a directory on the DFS that the output is written to.
    /// </summary>
    /// <value>The path of a directory on the DFS.</value>
    public string Path
    {
        get { return _path; }
    }

    /// <summary>
    /// Gets the type of the record writer.
    /// </summary>
    /// <value>The <see cref="Type"/> instance for the record writer. This will be a type inheriting from <see cref="RecordWriter{T}"/> where T equals <see cref="RecordType"/>.</value>
    public Type RecordWriterType
    {
        get { return _recordWriterType; }
    }

    /// <summary>
    /// Gets the type of the records that can be written to this output.
    /// </summary>
    /// <value>
    /// The type of the records.
    /// </value>
    public Type RecordType
    {
        get { return _recordType; }
    }

    /// <summary>
    /// Gets or sets the block size in bytes for the output files.
    /// </summary>
    /// <value>The size of the block in bytes, or 0 to use the DFS default setting. The default value is 0.</value>
    public int BlockSize { get; set; }

    /// <summary>
    /// Gets or sets the replication factor for the output files.
    /// </summary>
    /// <value>The replication factor, or 0 to the use the DFS default setting. The default value is 0.</value>
    public int ReplicationFactor { get; set; }

    /// <summary>
    /// Gets or sets the record options for the output files.
    /// </summary>
    /// <value>A combination of values from the <see cref="RecordStreamOptions"/> enumeration. The default value is <see cref="RecordStreamOptions.None"/>.</value>
    public RecordStreamOptions RecordOptions { get; set; }

    void IOperationOutput.ApplyOutput(FileSystemClient fileSystem, StageConfiguration stage)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(stage);

        stage.DataOutput = new FileDataOutput(fileSystem.Configuration, RecordWriterType, Path, BlockSize, ReplicationFactor, RecordOptions);
    }
}
