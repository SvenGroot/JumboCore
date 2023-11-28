// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using System.IO;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.IO;

/// <summary>
/// Writes stage output to a file system.
/// </summary>
/// <remarks>
/// <para>
///   This type inherits from <see cref="Configurable"/>, but the configuration is only used during task execution.
///   If you are creating a job configuration, there is no need to configure this type other than specifying
///   the <see cref="DfsConfiguration"/> to the <see cref="FileDataOutput.FileDataOutput(DfsConfiguration,Type,string,int,int,RecordStreamOptions)"/> constructor.
/// </para>
/// </remarks>
public class FileDataOutput : Configurable, IDataOutput
{
    /// <summary>
    /// The key of the setting in the stage settings that stores the output path format. You should not normally change this setting.
    /// </summary>
    public const string OutputPathFormatSettingKey = "FileDataOutput.OutputPathFormat";
    /// <summary>
    /// The key of the setting in the stage settings that stores the record writer type. You should not normally change this setting.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "TypeSetting")]
    public const string RecordWriterTypeSettingKey = "FileDataOutput.RecordWriterType";
    /// <summary>
    /// The key of the setting in the stage settings that stores the block size. You should not normally change this setting.
    /// </summary>
    public const string BlockSizeSettingKey = "FileDataOutput.BlockSizeSettingKey";
    /// <summary>
    /// The key of the setting in the stage settings that stores the replication factor. You should not normally change this setting.
    /// </summary>
    public const string ReplicationFactorSettingKey = "FileDataOutput.ReplicationFactor";
    /// <summary>
    /// The key of the setting in the stage settings that stores the record options. You should not normally change this setting.
    /// </summary>
    public const string RecordOptionsSettingKey = "FileDataOutput.RecordOptions";

    private readonly string? _outputPath;
    private Type? _recordWriterType;
    private int _blockSize;
    private int _replicationFactor;
    private RecordStreamOptions _recordOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileDataOutput"/> class.
    /// </summary>
    public FileDataOutput()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FileDataOutput" /> class.
    /// </summary>
    /// <param name="dfsConfiguration">The DFS configuration.</param>
    /// <param name="recordWriterType">Type of the record writer.</param>
    /// <param name="outputPath">The path of the directory to write the output to.</param>
    /// <param name="blockSize">The size of the output files' blocks, or 0 to use the default block size.</param>
    /// <param name="replicationFactor">The output files' replication factor, or 0 to use the default replication factor.</param>
    /// <param name="recordOptions">The <see cref="RecordStreamOptions" /> for the output.</param>
    public FileDataOutput(DfsConfiguration dfsConfiguration, Type recordWriterType, string outputPath, int blockSize = 0, int replicationFactor = 0, RecordStreamOptions recordOptions = RecordStreamOptions.None)
    {
        ArgumentNullException.ThrowIfNull(dfsConfiguration);
        ArgumentNullException.ThrowIfNull(recordWriterType);
        ArgumentNullException.ThrowIfNull(outputPath);
        if (blockSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(blockSize));
        }

        if (replicationFactor < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(replicationFactor));
        }

        if (recordWriterType.FindGenericBaseType(typeof(RecordWriter<>), false) == null)
        {
            throw new ArgumentException("The type is not a record writer.", nameof(recordWriterType));
        }

        if (FileSystemClient.Create(dfsConfiguration).GetDirectoryInfo(outputPath) == null)
        {
            throw new DirectoryNotFoundException(string.Format(CultureInfo.CurrentCulture, "The directory '{0}' does not exist.", outputPath));
        }

        DfsConfiguration = dfsConfiguration;
        _recordWriterType = recordWriterType;
        _outputPath = outputPath;
        _blockSize = blockSize;
        _replicationFactor = replicationFactor;
        _recordOptions = recordOptions;
    }

    /// <summary>
    /// Gets the type of the records used for this output.
    /// </summary>
    /// <value>
    /// The type of the records.
    /// </value>
    public Type RecordType
    {
        get { return RecordWriter.GetRecordType(_recordWriterType!); }
    }

    /// <summary>
    /// Creates the output for the specified partition.
    /// </summary>
    /// <param name="partitionNumber">The partition number for this output.</param>
    /// <returns>
    /// The record writer.
    /// </returns>
    public IOutputCommitter CreateOutput(int partitionNumber)
    {
        if (TaskContext == null || DfsConfiguration == null || JetConfiguration == null)
        {
            throw new InvalidOperationException("No task configuration stored in this instance.");
        }

        var fileSystem = FileSystemClient.Create(DfsConfiguration);
        // Must use TaskAttemptId for temp file name, there could be other attempts of this task writing the same data (only one will commit, of course).
        var tempFileName = fileSystem.Path.Combine(fileSystem.Path.Combine(TaskContext.DfsJobDirectory, "temp"), string.Format(CultureInfo.InvariantCulture, "{0}_partition{1}", TaskContext.TaskAttemptId, partitionNumber));
        var outputFileName = GetOutputPath(TaskContext.StageConfiguration, partitionNumber);
        var outputStream = fileSystem.CreateFile(tempFileName, _blockSize, _replicationFactor, _recordOptions);
        var writer = (IRecordWriter)JetActivator.CreateInstance(_recordWriterType!, DfsConfiguration, JetConfiguration, TaskContext, outputStream);
        return new FileOutputCommitter(writer, tempFileName, outputFileName);
    }

    /// <summary>
    /// Notifies the data input that it has been added to a stage.
    /// </summary>
    /// <param name="stage">The stage configuration of the stage.</param>
    public void NotifyAddedToStage(StageConfiguration stage)
    {
        ArgumentNullException.ThrowIfNull(stage);
        if (_outputPath == null)
        {
            throw new InvalidOperationException("No data output configuration is stored in this instance.");
        }

        stage.AddSetting(RecordWriterTypeSettingKey, _recordWriterType!.AssemblyQualifiedName!);
        var outputPathFormat = FileSystemClient.Create(DfsConfiguration!).Path.Combine(_outputPath, stage.StageId + "-{0:00000}");
        stage.AddSetting(OutputPathFormatSettingKey, outputPathFormat);
        if (_blockSize != 0)
        {
            stage.AddSetting(BlockSizeSettingKey, _blockSize);
        }

        if (_replicationFactor != 0)
        {
            stage.AddSetting(ReplicationFactorSettingKey, _replicationFactor);
        }

        if (_recordOptions != RecordStreamOptions.None)
        {
            stage.AddSetting(RecordOptionsSettingKey, _recordOptions);
        }
    }

    /// <summary>
    /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration" /> calls this method
    /// after setting the configuration.
    /// </summary>
    public override void NotifyConfigurationChanged()
    {
        base.NotifyConfigurationChanged();
        if (TaskContext != null && TaskContext.StageConfiguration.TryGetSetting(RecordWriterTypeSettingKey, out string? typeName))
        {
            _recordWriterType = Type.GetType(typeName, true)!;
            _blockSize = TaskContext.StageConfiguration.GetSetting(FileDataOutput.BlockSizeSettingKey, 0);
            _replicationFactor = TaskContext.StageConfiguration.GetSetting(FileDataOutput.ReplicationFactorSettingKey, 0);
            _recordOptions = TaskContext.StageConfiguration.GetSetting(FileDataOutput.RecordOptionsSettingKey, RecordStreamOptions.None);
        }
    }

    /// <summary>
    /// Gets the output path for the specified partition.
    /// </summary>
    /// <param name="stage">The stage configuration for the stage.</param>
    /// <param name="partitionNumber">The partition number.</param>
    /// <returns>The path of the output file for this partition.</returns>
    public static string GetOutputPath(StageConfiguration stage, int partitionNumber)
    {
        ArgumentNullException.ThrowIfNull(stage);
        var outputPathFormat = stage.GetSetting(FileDataOutput.OutputPathFormatSettingKey, null);
        if (outputPathFormat == null)
        {
            throw new InvalidOperationException("The stage settings do not contain an output path format.");
        }

        return string.Format(CultureInfo.InvariantCulture, outputPathFormat, partitionNumber);
    }
}
