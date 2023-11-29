// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Compiles the job information assembled by a <see cref="JobBuilder"/> into a <see cref="JobConfiguration"/>.
/// </summary>
public sealed class JobBuilderCompiler
{
    private readonly JobConfiguration _job;
    private readonly HashSet<string> _stageIds = new HashSet<string>();
    private readonly FileSystemClient _fileSystemClient;
    private readonly JetClient _jetClient;

    internal JobBuilderCompiler(IEnumerable<Assembly> assemblies, FileSystemClient fileSystemClient, JetClient jetClient)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        ArgumentNullException.ThrowIfNull(fileSystemClient);
        ArgumentNullException.ThrowIfNull(jetClient);

        _job = new JobConfiguration(assemblies);
        _fileSystemClient = fileSystemClient;
        _jetClient = jetClient;
    }

    /// <summary>
    /// Gets the job configuration created by the compiler.
    /// </summary>
    /// <value>
    /// The job configuration.
    /// </value>
    public JobConfiguration Job
    {
        get { return _job; }
    }

    /// <summary>
    /// Gets the default number of tasks that is used for a stage with a channel
    /// that didn't set the number of tasks explicitly.
    /// </summary>
    /// <value>
    /// The default task count.
    /// </value>
    public int DefaultChannelInputTaskCount
    {
        get { return _jetClient.JobServer.GetMetrics().Capacity; }
    }

    /// <summary>
    /// Creates a stage with data input and adds it to the job.
    /// </summary>
    /// <param name="stageId">The stage ID.</param>
    /// <param name="taskType">The type for the stage's tasks.</param>
    /// <param name="input">The data input for the stage.</param>
    /// <param name="output">The output for the stage. May be <see langword="null"/>.</param>
    /// <returns>The <see cref="StageConfiguration"/> for the stage.</returns>
    public StageConfiguration CreateStage(string stageId, Type taskType, FileInput input, IOperationOutput? output)
    {
        ArgumentNullException.ThrowIfNull(stageId);
        ArgumentNullException.ThrowIfNull(taskType);
        ArgumentNullException.ThrowIfNull(input);

        stageId = CreateUniqueStageId(stageId);

        var stage = _job.AddDataInputStage(stageId, input.CreateStageInput(_fileSystemClient), taskType);
        if (output != null)
        {
            output.ApplyOutput(_fileSystemClient, stage);
        }

        return stage;
    }

    /// <summary>
    /// Creates a stage with optional channel input and adds it to the job.
    /// </summary>
    /// <param name="stageId">The stage ID.</param>
    /// <param name="taskType">The type for the stage's tasks.</param>
    /// <param name="taskCount">The number of tasks in he stage, or zero to use the default.</param>
    /// <param name="input">The input for the stage. May be <see langword="null" />.</param>
    /// <param name="output">The output for the stage. May be <see langword="null" />.</param>
    /// <param name="allowEmptyTaskReplacement">if set to <see langword="true" />, empty task replacement is allowed.</param>
    /// <param name="channelSettings">The settings applied to the sending stage of the <paramref name="input"/> channel if <paramref name="input"/> is not <see langword="null"/>. Not used if empty task replacement is performed.</param>
    /// <returns>
    /// The <see cref="StageConfiguration" /> for the stage.
    /// </returns>
    /// <remarks>
    /// If <paramref name="allowEmptyTaskReplacement" /> is <see langword="true" />, the <paramref name="input" /> specifies a pipeline channel without
    /// internal partitioning (<paramref name="taskCount" /> must be 1), and the input stage uses <see cref="EmptyTask{T}" /> this method will not create a new stage, but will change the task type
    /// of that stage with the specified task, rename the stage, and return the configuration of that stage.
    /// </remarks>
    public StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, InputStageInfo? input, IOperationOutput? output, bool allowEmptyTaskReplacement, SettingsDictionary? channelSettings)
    {
        ArgumentNullException.ThrowIfNull(stageId);
        ArgumentNullException.ThrowIfNull(taskType);

        StageConfiguration stage;
        if (input != null && allowEmptyTaskReplacement && taskCount <= 1 && input.ChannelType == Channels.ChannelType.Pipeline && IsEmptyTask(input.InputStage.TaskType.GetReferencedType()))
        {
            if (stageId != input.InputStage.StageId)
            {
                // Must ensure a unique name if input is not a child stage.
                if (input.InputStage.Parent == null)
                {
                    _stageIds.Remove(input.InputStage.StageId!);
                    stageId = CreateUniqueStageId(stageId);
                }
                _job.RenameStage(input.InputStage, stageId);
            }
            input.InputStage.TaskType = taskType;
            stage = input.InputStage;
        }
        else
        {
            // Must ensure a unique name if not a child stage.
            if (input == null || input.ChannelType != Channels.ChannelType.Pipeline)
            {
                stageId = CreateUniqueStageId(stageId);
            }

            if (input != null && channelSettings != null)
            {
                input.InputStage.AddSettings(channelSettings);
            }

            stage = _job.AddStage(stageId, taskType, DetermineTaskCount(taskCount, input), input);
        }

        if (output != null)
        {
            output.ApplyOutput(_fileSystemClient, stage);
        }

        return stage;
    }

    /// <summary>
    /// Creates a stage with more than one channel input.
    /// </summary>
    /// <param name="stageId">The stage ID.</param>
    /// <param name="taskType">The type for the stage's tasks.</param>
    /// <param name="taskCount">The number of tasks in he stage, or zero to use the default.</param>
    /// <param name="input">The input for the stage. May be <see langword="null" />.</param>
    /// <param name="output">The output for the stage. May be <see langword="null" />.</param>
    /// <param name="channelSettings">The settings applied to the sending stage of the <paramref name="input"/> channel if <paramref name="input"/> is not <see langword="null"/>. Not used if empty task replacement is performed.</param>
    /// <param name="stageMultiInputRecordReaderType">Type of the stage multi input record reader.</param>
    /// <returns>The stage configuration.</returns>
    public StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, InputStageInfo[] input, IOperationOutput? output, SettingsDictionary[] channelSettings, Type stageMultiInputRecordReaderType)
    {
        ArgumentNullException.ThrowIfNull(stageId);
        ArgumentNullException.ThrowIfNull(taskType);
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(channelSettings);
        if (input.Length == 0)
        {
            throw new ArgumentException("Empty input list.", nameof(input));
        }

        if (input.Length != channelSettings.Length)
        {
            throw new ArgumentException("Incorrect number of channel settings entries.");
        }

        ArgumentNullException.ThrowIfNull(stageMultiInputRecordReaderType);

        stageId = CreateUniqueStageId(stageId);
        for (var x = 0; x < input.Length; ++x)
        {
            if (channelSettings[x] != null)
            {
                input[x].InputStage.AddSettings(channelSettings[x]);
            }
        }

        var stage = _job.AddStage(stageId, taskType, taskCount == 0 ? DefaultChannelInputTaskCount : taskCount, input, stageMultiInputRecordReaderType);

        if (output != null)
        {
            output.ApplyOutput(_fileSystemClient, stage);
        }

        return stage;
    }

    internal static bool IsEmptyTask(Type type)
    {
        return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(EmptyTask<>);
    }

    private string CreateUniqueStageId(string stageId)
    {
        var result = stageId;
        var number = 2;
        while (_stageIds.Contains(result))
        {
            result = string.Format(CultureInfo.InvariantCulture, "{0}_{1}", stageId, number);
        }
        _stageIds.Add(result);
        return result;
    }

    private int DetermineTaskCount(int taskCount, InputStageInfo? input)
    {
        if (taskCount != 0)
        {
            return taskCount;
        }
        else if (input == null || input.ChannelType == ChannelType.Pipeline)
        {
            return 1;
        }
        else if (input != null && input.ChannelType == ChannelType.Tcp)
        {
            return DefaultChannelInputTaskCount / 2; // Don't use full capacity for TCP channel receiving stage because all must be scheduled simultaneously while still running sending stage tasks too.
        }
        else
        {
            return DefaultChannelInputTaskCount;
        }
    }
}
