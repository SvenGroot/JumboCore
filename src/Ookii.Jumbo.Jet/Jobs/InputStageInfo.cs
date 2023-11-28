// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet.Jobs;

/// <summary>
/// Provides information about an input stage to the <see cref="JobConfiguration.AddStage(string, Type, int, InputStageInfo)"/> method.
/// </summary>
public class InputStageInfo
{
    private Type? _partitionerType;
    private Type? _multiInputRecordReaderType;

    /// <summary>
    /// Initializes a new instance of the <see cref="InputStageInfo"/> class.
    /// </summary>
    /// <param name="inputStage">The stage configuration of the input stage.</param>
    public InputStageInfo(StageConfiguration inputStage)
    {
        ArgumentNullException.ThrowIfNull(inputStage);

        InputStage = inputStage;
        PartitionsPerTask = 1;
    }

    /// <summary>
    /// Gets the stage configuration of the input stage.
    /// </summary>
    public StageConfiguration InputStage { get; private set; }

    /// <summary>
    /// Gets the type of the channel to use.
    /// </summary>
    public ChannelType ChannelType { get; set; }

    /// <summary>
    /// Gets the type of partitioner to use.
    /// </summary>
    [AllowNull]
    public Type PartitionerType
    {
        get
        {
            return _partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(InputStageOutputType);
        }
        set { _partitionerType = value; }
    }

    /// <summary>
    /// Gets the number of partitions to create for each output task.
    /// </summary>
    public int PartitionsPerTask { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to disable dynamic partition assignment.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if dynamic partition assignment; otherwise, <see langword="false"/>.
    /// </value>
    /// <remarks>
    /// <para>
    ///   If <see cref="PartitionsPerTask"/> is larger than 1, tasks can dynamically receive additional partitions
    ///   besides those that were initially assigned to them.
    /// </para>
    /// <para>
    ///   If this property is set to <see langword="true"/>, dynamic partition assignment is disabled for
    ///   the receiving stage of this channel, and every task will only ever process the partitions it was
    ///   initially assigned.
    /// </para>
    /// </remarks>
    public bool DisableDynamicPartitionAssignment { get; set; }

    /// <summary>
    /// Gets or sets the method used to assign partitions to tasks when the job is started.
    /// </summary>
    /// <value>The partition assignment method.</value>
    public PartitionAssignmentMethod PartitionAssignmentMethod { get; set; }

    /// <summary>
    /// Gets the type of multi input record reader to use.
    /// </summary>
    [AllowNull]
    public Type MultiInputRecordReaderType
    {
        get
        {
            return _multiInputRecordReaderType ?? (ChannelType == ChannelType.Tcp ? typeof(RoundRobinMultiInputRecordReader<>).MakeGenericType(InputStageOutputType) : typeof(MultiRecordReader<>).MakeGenericType(InputStageOutputType));
        }
        set { _multiInputRecordReaderType = value; }
    }

    private Type InputStageOutputType
    {
        get { return InputStage.TaskTypeInfo!.OutputRecordType; }
    }

    private void ValidatePartitionerType()
    {
        // Get the output type of the input stage, which is the input to the partitioner.
        var inputType = InputStageOutputType;
        var partitionerInterfaceType = PartitionerType.FindGenericInterfaceType(typeof(IPartitioner<>))!;
        var partitionedType = partitionerInterfaceType.GetGenericArguments()[0];
        if (partitionedType != inputType)
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The partitioner type {0} cannot partition objects of type {1}.", PartitionerType, inputType));
        }
    }

    internal void ValidateTypes(Type? stageMultiInputRecordReaderType, Type inputType)
    {
        ValidatePartitionerType();
        ValidateMultiInputRecordReaderType(stageMultiInputRecordReaderType, inputType);
    }

    private void ValidateMultiInputRecordReaderType(Type? stageMultiInputRecordReaderType, Type inputType)
    {
        IEnumerable<Type> acceptedInputTypes;
        Type recordType;
        if (stageMultiInputRecordReaderType != null)
        {
            // The output of the stage multi input record reader type must match the input type of the stage.
            recordType = RecordReader.GetRecordType(stageMultiInputRecordReaderType);
            if (recordType != inputType)
            {
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified stage multi input record reader type {0} doesn't return objects of type {1}.", stageMultiInputRecordReaderType, inputType), nameof(stageMultiInputRecordReaderType));
            }

            acceptedInputTypes = MultiInputRecordReader.GetAcceptedInputTypes(MultiInputRecordReaderType);
        }
        else
        {
            acceptedInputTypes = new[] { inputType };
        }

        var stageOutputType = InputStageOutputType;
        recordType = RecordReader.GetRecordType(MultiInputRecordReaderType);
        if (!acceptedInputTypes.Contains(recordType))
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified channel multi input record reader type {0} doesn't return objects of the correct type.", MultiInputRecordReaderType));
        }

        var channelAcceptedInputTypes = MultiInputRecordReader.GetAcceptedInputTypes(MultiInputRecordReaderType);
        if (!channelAcceptedInputTypes.Contains(stageOutputType))
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified channel multi input record reader type {0} doesn't accept objects of the correct type.", MultiInputRecordReaderType));
        }
    }

}
