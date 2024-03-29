﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// Represents the writing end of a file channel between two tasks.
/// </summary>
public sealed class FileOutputChannel : OutputChannel, IHasMetrics
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileOutputChannel));

    private readonly string _localJobDirectory;
    private IRecordWriter? _writer;
    private readonly FileChannelOutputType _outputType;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileOutputChannel"/> class.
    /// </summary>
    /// <param name="taskExecution">The task execution utility for the task that this channel is for.</param>
    public FileOutputChannel(TaskExecutionUtility taskExecution)
        : base(taskExecution)
    {
        ArgumentNullException.ThrowIfNull(taskExecution);
        var root = taskExecution.RootTask;

        // We don't include child task IDs in the output file name because internal partitioning can happen only once
        // so the number always matches the output partition number anyway.
        var inputTaskAttemptId = root.Context.TaskAttemptId.ToString();
        _localJobDirectory = taskExecution.Context.LocalJobDirectory;
        var directory = Path.Combine(_localJobDirectory, inputTaskAttemptId);
        if (!Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _outputType = taskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.Spill);
        _log.DebugFormat("File channel output type: {0}", _outputType);
    }

    /// <summary>
    /// Gets the number of bytes read from the local disk.
    /// </summary>
    /// <value>The local bytes read.</value>
    public long LocalBytesRead
    {
        get
        {
            return 0;
        }
    }

    /// <summary>
    /// Gets the number of bytes written to the local disk.
    /// </summary>
    /// <value>The local bytes written.</value>
    public long LocalBytesWritten
    {
        get
        {
            if (_writer == null)
            {
                return 0;
            }
            else
            {
                return _writer.BytesWritten;
            }
        }
    }

    /// <summary>
    /// Gets the number of bytes read over the network.
    /// </summary>
    /// <value>The network bytes read.</value>
    /// <remarks>Only channels should normally use this property.</remarks>
    public long NetworkBytesRead
    {
        get { return 0; }
    }

    /// <summary>
    /// Gets the number of bytes written over the network.
    /// </summary>
    /// <value>The network bytes written.</value>
    /// <remarks>Only channels should normally use this property.</remarks>
    public long NetworkBytesWritten
    {
        get { return 0; }
    }

    /// <summary>
    /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
    public override RecordWriter<T> CreateRecordWriter<T>()
    {
        if (_writer != null)
        {
            throw new InvalidOperationException("The channel record writer has already been created.");
        }

        var writeBufferSize = TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.WriteBufferSize, TaskExecution.JetClient.Configuration.FileChannel.WriteBufferSize);

        return CreateSpillRecordWriter<T>(writeBufferSize);
    }

    /// <summary>
    /// Creates the name of an intermediate file for the channel. For Jumbo internal use only.
    /// </summary>
    /// <param name="inputTaskAttemptId">The input task attempt id.</param>
    /// <returns>The intermediate file name.</returns>
    public static string CreateChannelFileName(string inputTaskAttemptId)
    {
        return Path.Combine(inputTaskAttemptId, inputTaskAttemptId + ".output");
    }

    private RecordWriter<T> CreateSpillRecordWriter<T>(BinarySize writeBufferSize)
        where T : notnull
    {
        // We're using single file output

        var outputBufferSize = TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferSize, TaskExecution.JetClient.Configuration.FileChannel.SpillBufferSize);
        var outputBufferLimit = TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.SpillBufferLimit, TaskExecution.JetClient.Configuration.FileChannel.SpillBufferLimit);
        if (outputBufferSize.Value < 0 || outputBufferSize.Value > Int32.MaxValue)
        {
            throw new ConfigurationErrorsException("Invalid output buffer size: " + outputBufferSize.Value);
        }

        if (outputBufferLimit < 0.1f || outputBufferLimit > 1.0f)
        {
            throw new ConfigurationErrorsException("Invalid output buffer limit: " + outputBufferLimit);
        }

        var outputBufferLimitSize = (int)(outputBufferLimit * outputBufferSize.Value);

        _log.DebugFormat(CultureInfo.InvariantCulture, "Creating {3} output writer with buffer: {0}; limit: {1}; write buffer: {2}.", outputBufferSize.Value, outputBufferLimitSize, writeBufferSize.Value, _outputType);

        var partitioner = CreatePartitioner<T>();
        partitioner.Partitions = OutputPartitionIds.Count;
        RecordWriter<T> result;
        var fileName = CreateChannelFileName(TaskExecution.RootTask.Context.TaskAttemptId.ToString());
        if (_outputType == FileChannelOutputType.SortSpill)
        {
            var maxDiskInputsPerMergePass = TaskExecution.Context.GetSetting(MergeRecordReaderConstants.MaxFileInputsSetting, TaskExecution.JetClient.Configuration.MergeRecordReader.MaxFileInputs);
            var combiner = (ITask<T, T>?)CreateCombiner();
            var comparer = (IComparer<T>?)CreateComparer();
            var minSpillCountForCombineDuringMerge = TaskExecution.Context.GetSetting(JumboSettings.FileChannel.StageOrJob.SpillSortMinSpillsForCombineDuringMerge, TaskExecution.JetClient.Configuration.FileChannel.SpillSortMinSpillsForCombineDuringMerge);
            result = new SortSpillRecordWriter<T>(Path.Combine(_localJobDirectory, fileName), partitioner, (int)outputBufferSize.Value, outputBufferLimitSize, (int)writeBufferSize.Value, TaskExecution.JetClient.Configuration.FileChannel.EnableChecksum, CompressionType, maxDiskInputsPerMergePass, comparer, combiner, minSpillCountForCombineDuringMerge);
        }
        else
        {
            result = new SingleFileMultiRecordWriter<T>(Path.Combine(_localJobDirectory, fileName), partitioner, (int)outputBufferSize.Value, outputBufferLimitSize, (int)writeBufferSize.Value, TaskExecution.JetClient.Configuration.FileChannel.EnableChecksum, CompressionType);
        }

        _writer = result;
        return result;
    }

    private object? CreateCombiner()
    {
        var combinerTypeName = TaskExecution.Context.StageConfiguration.GetSetting(JumboSettings.FileChannel.Stage.SpillSortCombinerType, null);
        if (combinerTypeName == null)
        {
            return null;
        }

        var combinerType = Type.GetType(combinerTypeName, true)!;
        return JetActivator.CreateInstance(combinerType, TaskExecution);
    }

    private object? CreateComparer()
    {
        var comparerTypeName = TaskExecution.Context.StageConfiguration.GetSetting(JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
        if (comparerTypeName == null)
        {
            return null;
        }

        var comparerType = Type.GetType(comparerTypeName, true)!;
        return JetActivator.CreateInstance(comparerType, TaskExecution);
    }
}
