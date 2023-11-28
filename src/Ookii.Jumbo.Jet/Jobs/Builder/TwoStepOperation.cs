// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.Tasks;

namespace Ookii.Jumbo.Jet.Jobs.Builder;

/// <summary>
/// Represents a job builder operation that may consist of two steps.
/// </summary>
/// <remarks>
/// <para>
///   Sorting is an example of a two step operation: first each input is locally sorted, and then the result is merged on the receiving side of the channel.
/// </para>
/// <para>
///   The additional step is only created when necessary. If the input is a channel with only one task in the sending stage (or a file input with only one split),
///   or the channel type is explicitly set to pipeline no additional step is created.
/// </para>
/// <para>
///   Any settings specified in the <see cref="StageOperationBase.Settings"/> property will be applied to both stages created for this step.
/// </para>
/// </remarks>
public class TwoStepOperation : StageOperation
{
    private readonly TaskTypeInfo _secondStepTaskType;
    private readonly bool _usePrePartitioning;

    /// <summary>
    /// Initializes a new instance of the <see cref="TwoStepOperation"/> class.
    /// </summary>
    /// <param name="builder">The job builder.</param>
    /// <param name="input">The input for the operation.</param>
    /// <param name="taskType">Type of the task. May be a generic type definition with a single type parameter.</param>
    /// <param name="secondStepTaskType">The type of the task for the second step. May be a generic type definition with a single type parameter. May be <see langword="null"/> to use the same type as <paramref name="taskType"/>.</param>
    /// <param name="usePrePartitioning">If set to <see langword="true"/> the input to the first step will be partitioned when a second step is created.</param>
    /// <remarks>
    /// <para>
    ///   If <paramref name="taskType"/> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
    ///   You can use this with types such as <see cref="Tasks.EmptyTask{T}"/>, in which case you can specify them as <c>typeof(EmptyTask&lt;&gt;)</c> without
    ///   specifying the record type.
    /// </para>
    /// <para>
    ///   For <paramref name="secondStepTaskType"/> the same thing is done by using the output record type of the <paramref name="taskType"/>.
    /// </para>
    /// </remarks>
    public TwoStepOperation(JobBuilder builder, IOperationInput input, Type taskType, Type? secondStepTaskType, bool usePrePartitioning)
        : base(builder, CreateExtraStepForDataInput(builder, input), taskType)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (secondStepTaskType != null)
        {
            if (secondStepTaskType.IsGenericTypeDefinition)
            {
                secondStepTaskType = secondStepTaskType.MakeGenericType(TaskType.OutputRecordType);
            }

            _secondStepTaskType = new TaskTypeInfo(secondStepTaskType);
            if (!(_secondStepTaskType.InputRecordType == TaskType.OutputRecordType && _secondStepTaskType.OutputRecordType == TaskType.OutputRecordType))
            {
                throw new ArgumentException("The second step task type is incompatible with the first step task type.");
            }
        }
        else
        {
            _secondStepTaskType = TaskType;
        }

        _usePrePartitioning = usePrePartitioning;
    }

    /// <summary>
    /// Gets or sets the stage ID for the second step, if one is created.
    /// </summary>
    /// <value>
    /// The second step stage ID, or <see langword="null"/> to use <see cref="StageOperationBase.StageId"/>.
    /// </value>
    public string? SecondStepStageId { get; set; }


    /// <summary>
    /// Gets the type of the task for the second step, if one is created.
    /// </summary>
    /// <value>
    /// The type of the second step's task.
    /// </value>
    public TaskTypeInfo SecondStepTaskType
    {
        get { return _secondStepTaskType; }
    }

    /// <summary>
    /// Gets the <see cref="StageConfiguration"/> for the first step.
    /// </summary>
    /// <value>
    /// The <see cref="StageConfiguration"/> for the first step, or <see langword="null" /> if the stage hasn't been compiled yet.
    /// </value>
    /// <remarks>
    /// <para>
    ///   If a second step is created, <see cref="IJobBuilderOperation.Stage"/> will return the second step. This property can be used to access the first step's configuration.
    /// </para>
    /// <para>
    ///   If no second step was created, the value of <see cref="FirstStepStage"/> will be the same as <see cref="IJobBuilderOperation.Stage"/>.
    /// </para>
    /// </remarks>
    protected StageConfiguration? FirstStepStage { get; set; }

    /// <summary>
    /// Creates the configuration for this stage.
    /// </summary>
    /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
    /// <returns>
    /// The <see cref="StageConfiguration"/> for the stage.
    /// </returns>
    protected override StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
    {
        ArgumentNullException.ThrowIfNull(compiler);
        // We don't need an extra step if each of our tasks would get only a single input segment, i.e. when
        // our input channel is a pipeline or has only one task.
        if (InputChannel!.ChannelType != ChannelType.Pipeline && InputChannel.Sender.Stage.Root.TaskCount > 1)
        {
            // Second step needed
            var taskCount = (_usePrePartitioning && InputChannel.Sender.Stage.InternalPartitionCount == 1) ? InputChannel.PartitionCount : 1;
            if (taskCount == 0)
            {
                taskCount = InputChannel.PartitionsPerTask * compiler.DefaultChannelInputTaskCount;
                if (InputChannel.ChannelType == ChannelType.Tcp)
                {
                    taskCount /= 2;
                }
            }
            var input = new InputStageInfo(InputChannel.Sender.Stage)
            {
                ChannelType = ChannelType.Pipeline,
                PartitionerType = InputChannel.PartitionerType
            };
            var firstStepStageId = StageId;
            if (SecondStepStageId == null)
            {
                firstStepStageId = "Local" + StageId;
            }

            FirstStepStage = compiler.CreateStage(firstStepStageId, TaskType.TaskType, taskCount, input, InputChannel, true, null);
            // Settings are only automatically applied to the returned stage; manually apply them here.
            FirstStepStage.AddSettings(Settings);
            input = InputChannel.CreateInput(FirstStepStage);
            Debug.Assert(input.ChannelType != ChannelType.Pipeline);
            return compiler.CreateStage(SecondStepStageId ?? StageId, _secondStepTaskType.TaskType, InputChannel.TaskCount, input, Output, true, InputChannel.Settings);
        }
        else
        {
            FirstStepStage = base.CreateConfiguration(compiler);
            return FirstStepStage;
        }
    }

    private static IOperationInput CreateExtraStepForDataInput(JobBuilder builder, IOperationInput input)
    {
        var dataInput = input as FileInput;
        if (dataInput != null)
        {
            // If the input is DFS, we want to create a channel around which our first and second step are created.
            // Here's the fun bit: if the input has only one split (so there is only one task), the compiler will
            // decide that an extra step isn't necessary, and just append our regular taskType to this stage.
            // Unless the channel was customized, this will then replace the EmptyTask so there's no extra step in the final job.
            return new StageOperation(builder, input, typeof(EmptyTask<>)) { StageId = "ReadStage" };
        }

        return input;
    }
}
