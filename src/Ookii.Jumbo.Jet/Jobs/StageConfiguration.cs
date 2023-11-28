// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Xml.Serialization;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Jobs;

/// <summary>
/// Provides the configuration for a stage in a job. A stage is a collection of tasks that perform the same function
/// but on different inputs.
/// </summary>
[XmlType("Stage", Namespace = JobConfiguration.XmlNamespace)]
public class StageConfiguration
{
    private string? _stageId;
    private int _taskCount;
    private readonly ExtendedCollection<string> _dependentStages = new ExtendedCollection<string>();
    private StageConfiguration? _childStage;
    private IDataInput? _dataInput;
    private IDataOutput? _dataOutput;
    private TypeReference _taskType;
    private TaskTypeInfo? _taskTypeInfo;

    /// <summary>
    /// Initializes a new instance of the <see cref="StageConfiguration"/> class.
    /// </summary>
    public StageConfiguration()
    {
    }

    /// <summary>
    /// Gets or sets the unique identifier for the stage.
    /// </summary>
    [XmlAttribute("id")]
    public string? StageId
    {
        get { return _stageId; }
        set
        {
            if (value != null && value.IndexOfAny(new char[] { TaskId.ChildStageSeparator, TaskId.TaskNumberSeparator }) >= 0)
            {
                throw new ArgumentException("A stage ID cannot contain the character '.', '-' or '_'.", nameof(value));
            }

            _stageId = value;
        }
    }

    /// <summary>
    /// Gets or sets the type that implements the task.
    /// </summary>
    public TypeReference TaskType
    {
        get { return _taskType; }
        set
        {
            _taskType = value;
            _taskTypeInfo = null;
        }
    }

    /// <summary>
    /// Gets information about the task type.
    /// </summary>
    /// <value>
    /// The <see cref="TaskTypeInfo"/> for the <see cref="TaskType"/>, or <see langword="null"/> if the type has not been set.
    /// </value>
    [XmlIgnore]
    public TaskTypeInfo? TaskTypeInfo => _taskTypeInfo ??= (_taskType.TryGetReferencedType(out var type) ? new(type) : null);

    /// <summary>
    /// Gets or sets the number of tasks in this stage.
    /// </summary>
    /// <remarks>
    /// This property is ignored if <see cref="DataInput"/> is not <see langword="null"/>.
    /// </remarks>
    [XmlAttribute("taskCount")]
    public int TaskCount
    {
        get
        {
            if (DataInput != null)
            {
                return DataInput.TaskInputs!.Count;
            }

            return _taskCount;
        }
        set
        {
            _taskCount = value;
        }
    }

    /// <summary>
    /// Gets or sets the input for this stage.
    /// </summary>
    /// <value>
    /// The input for the stage, or <see langword="null"/> if the stage has no input or channel input, or the job configuration was loaded from XML.
    /// </value>
    /// <remarks>
    /// <note>
    ///   This value is not saved in the job configuration, and will not be available after loading a job configuration.
    ///   Instead, the type of this property will be saved in <see cref="DataInputType"/>.
    /// </note>
    /// <note>
    ///   Don't set this property manually while constructing a job. Instead, use the <see cref="JobConfiguration.AddDataInputStage"/> method.
    /// </note>
    /// </remarks>
    [XmlIgnore]
    public IDataInput? DataInput
    {
        get { return _dataInput; }
        set
        {
            // We can do validation here because this is not a serialized property.
            if (value != null && TaskTypeInfo != null)
            {
                ValidateInputType(value, TaskTypeInfo);
            }

            _dataInput = value;
            DataInputType = value == null ? TypeReference.Empty : new TypeReference(value.GetType());
            if (value != null)
            {
                value.NotifyAddedToStage(this);
            }
        }
    }

    /// <summary>
    /// Gets or sets the <see cref="Type"/> of the <see cref="IDataInput"/> used by this stage.
    /// </summary>
    /// <value>
    /// The type of the input, or <see langword="null"/> if the stage has no input or channel input.
    /// </value>
    /// <remarks>
    /// <note>
    ///   Don't set this property manually while constructing a job. Instead, use the <see cref="JobConfiguration.AddDataInputStage"/> method.
    /// </note>
    /// </remarks>
    public TypeReference DataInputType { get; set; }


    /// <summary>
    /// Gets a value indicating whether this stage has input other than a channel.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if this instance has input; otherwise, <see langword="false"/>.
    /// </value>
    [XmlIgnore]
    public bool HasDataInput
    {
        get { return !string.IsNullOrEmpty(DataInputType.TypeName); }
    }

    /// <summary>
    /// Gets or sets the data output for this stage.
    /// </summary>
    /// <value>
    /// The output for the stage, or <see langword="null"/> if the stage has no output or channel output, or the job configuration was loaded from XML.
    /// </value>
    /// <remarks>
    /// <note>
    ///   This value is not saved in the job configuration, and will not be available after loading a job configuration.
    ///   Instead, the type of this property will be saved in <see cref="DataInputType"/>.
    /// </note>
    /// </remarks>
    [XmlIgnore]
    public IDataOutput? DataOutput
    {
        get { return _dataOutput; }
        set
        {
            if (value == null)
            {
                _dataInput = null;
                DataOutputType = TypeReference.Empty;
            }
            else
            {
                if (OutputChannel != null || ChildStage != null)
                {
                    throw new InvalidOperationException("Cannot add data output to a stage that already has an output channel.");
                }

                if (TaskTypeInfo != null)
                {
                    ValidateOutputType(value, TaskTypeInfo);
                }

                _dataOutput = value;
                DataOutputType = value.GetType();
                value.NotifyAddedToStage(this);
            }
        }
    }

    /// <summary>
    /// Gets or sets the <see cref="Type"/> of the <see cref="IDataOutput"/> used by this stage.
    /// </summary>
    /// <value>
    /// The type of the input, or <see langword="null"/> if the stage has no output or channel output.
    /// </value>
    /// <remarks>
    /// <note>
    ///   Don't set this property manually while constructing a job. Instead, use the <see cref="DataOutput"/> property.
    /// </note>
    /// </remarks>
    public TypeReference DataOutputType { get; set; }

    /// <summary>
    /// Gets a value indicating whether this stage has an <see cref="IDataOutput"/> to which the output is written.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if this instance has data output; otherwise, <see langword="false"/>.
    /// </value>
    [XmlIgnore]
    public bool HasDataOutput
    {
        get { return !string.IsNullOrEmpty(DataOutputType.TypeName); }
    }

    /// <summary>
    /// Gets or sets a child stage that will be connected to this stage's tasks via a <see cref="Channels.PipelineOutputChannel"/>.
    /// </summary>
    public StageConfiguration? ChildStage
    {
        get { return _childStage; }
        set
        {
            if (_childStage != value)
            {
                if (value != null && value.Parent != null)
                {
                    throw new ArgumentException("The item already has a parent.");
                }

                if (_childStage != null)
                {
                    _childStage.Parent = null;
                }

                _childStage = value;
                if (_childStage != null)
                {
                    _childStage.Parent = this;
                }
            }
        }
    }

    /// <summary>
    /// Gets the parent of this instance.
    /// </summary>
    [XmlIgnore]
    public StageConfiguration? Parent { get; private set; }

    /// <summary>
    /// Gets the root stage of this compound stage.
    /// </summary>
    /// <value>The root.</value>
    [XmlIgnore]
    public StageConfiguration Root
    {
        get
        {
            var root = this;
            while (root.Parent != null)
            {
                root = root.Parent;
            }

            return root;
        }
    }

    /// <summary>
    /// Gets the deepest nested child stage of this compound stage.
    /// </summary>
    /// <value>The leaf child stage.</value>
    [XmlIgnore]
    public StageConfiguration Leaf
    {
        get
        {
            var leaf = this;
            while (leaf.ChildStage != null)
            {
                leaf = leaf.ChildStage;
            }

            return leaf;
        }
    }

    /// <summary>
    /// Gets or sets the name of the type of the partitioner to use to partitioner elements amount the child stages' tasks.
    /// </summary>
    public TypeReference ChildStagePartitionerType { get; set; }

    /// <summary>
    /// Gets or sets a list of settings that are specific to this task.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
    public SettingsDictionary? StageSettings { get; set; }

    /// <summary>
    /// Gets or sets the output channel configuration for this stage.
    /// </summary>
    public ChannelConfiguration? OutputChannel { get; set; }

    /// <summary>
    /// Gets or sets the type of multi record reader to use when there are multiple channels with this stage as output stage.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   Whereas the <see cref="ChannelConfiguration.MultiInputRecordReaderType"/> property of the <see cref="ChannelConfiguration"/> class is used to specify
    ///   the multi input record reader to use to combine the output of all the tasks in the channel's input stage, this property is used to indicate
    ///   how the output of the input stages of this stage should be combined, if there is more than one.
    /// </para>
    /// </remarks>
    public TypeReference MultiInputRecordReaderType { get; set; }

    /// <summary>
    /// Gets the IDs of stages that have a dependency on this stage that is not represented by a channel.
    /// </summary>
    /// <value>The IDs of the dependent stages.</value>
    /// <remarks>
    /// <para>
    ///   In some cases, a stage may depend on the work done by another stage in a way that cannot be
    ///   represented by a channel. For example, if the stage requires DFS output that was produced
    ///   by that stage, it must not be scheduled before that stage finishes even though there is no
    ///   channel between them.
    /// </para>
    /// </remarks>
    public Collection<string> DependentStages
    {
        get { return _dependentStages; }
    }

    /// <summary>
    /// Gets a value that indicates whether the task type allows reusing the same object instance for every record.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   If this property is <see langword="true"/>, it means that the record reader (or if this stage is a child stage,
    ///   the parent stage) that provides the input records for this stage can reuse the same object instance for every record.
    /// </para>
    /// <para>
    ///   This property will return <see langword="true"/> if the <see cref="AllowRecordReuseAttribute"/> is defined on the <see cref="TaskType"/>.
    ///   If the <see cref="AllowRecordReuseAttribute.PassThrough"/> property is <see langword="true"/>, then this property will return <see langword="true"/>
    ///   only if the <see cref="AllowOutputRecordReuse"/> property is <see langword="true" />.
    /// </para>
    /// </remarks>
    [XmlIgnore]
    public bool AllowRecordReuse
    {
        get
        {
            if (TaskTypeInfo == null)
            {
                return false;
            }

            switch (TaskTypeInfo.RecordReuse)
            {
            case TaskRecordReuse.Allowed:
                return true;
            case TaskRecordReuse.PassThrough:
                return AllowOutputRecordReuse;
            default:
                return false;
            }
        }
    }

    /// <summary>
    /// Gets a value that indicates whether the tasks of this stage may re-use the same object instance when they
    /// write records to the output.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   This property will return <see langword="true"/> if this stage has no child stage, or if child stage's <see cref="AllowRecordReuse"/>
    ///   property is <see langword="true"/> and the child stage is a push task.
    /// </para>
    /// <para>
    ///   If you write a task type that may be used in multiple types of jobs, and you are not certain what the job configuration
    ///   the task type is used in will look like, you should check this property to see if you can re-use the same object instance
    ///   for the record passed to every call to <see cref="Ookii.Jumbo.IO.RecordWriter{T}.WriteRecord"/>. If this property is <see langword="false"/>, you must create
    ///   a new instance every time.
    /// </para>
    /// <para>
    ///   A child stage which isn't a push task doesn't support output record reuse because the pipeline channel for pull tasks doesn't support it.
    /// </para>
    /// </remarks>
    [XmlIgnore]
    public bool AllowOutputRecordReuse
    {
        get
        {
            return ChildStage?.TaskTypeInfo != null ? ChildStage.TaskTypeInfo.IsPushTask && ChildStage.AllowRecordReuse : true;
        }
    }

    /// <summary>
    /// Gets the compound stage ID.
    /// </summary>
    [XmlIgnore]
    public string? CompoundStageId
    {
        get
        {
            if (Parent == null)
            {
                return StageId;
            }
            else
            {
                return Parent.CompoundStageId + TaskId.ChildStageSeparator + StageId;
            }
        }
    }

    /// <summary>
    /// Gets the total number of partitions output from this stage. This does not include the output channel's partitioning, only the internal partitioning
    /// done by compound stages.
    /// </summary>
    /// <remarks>
    /// This number will be 1 unless this stage is a child stage in a compound stage, and partitioning occurs inside the compound stage before this stage.
    /// </remarks>
    [XmlIgnore]
    public int InternalPartitionCount
    {
        get
        {
            if (Parent == null)
            {
                return 1;
            }
            else
            {
                return Parent.InternalPartitionCount * TaskCount;
            }
        }
    }

    [XmlIgnore]
    internal bool IsOutputPrepartitioned
        => TaskType.TryGetReferencedType(out var type)
            && type.FindGenericBaseType(typeof(PrepartitionedPushTask<,>), false) != null;

    /// <summary>
    /// Gets a child stage of this stage.
    /// </summary>
    /// <param name="childStageId">The child stage ID.</param>
    /// <returns>The <see cref="StageConfiguration"/> for the child stage, or <see langword="null"/> if no stage with the specified name exists.</returns>
    public StageConfiguration? GetNamedChildStage(string childStageId)
    {
        ArgumentNullException.ThrowIfNull(childStageId);

        if (ChildStage != null && ChildStage.StageId == childStageId)
        {
            return ChildStage;
        }
        else
        {
            return null;
        }
    }

    /// <summary>
    /// Gets a setting with the specified type and default value.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
    /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
    public T? GetSetting<T>(string key, T? defaultValue)
    {
        if (StageSettings == null)
        {
            return defaultValue;
        }
        else
        {
            return StageSettings.GetSetting(key, defaultValue);
        }
    }

    /// <summary>
    /// Tries to get a setting with the specified type from the stage settings.
    /// </summary>
    /// <typeparam name="T">The type of the setting.</typeparam>
    /// <param name="key">The name of the setting..</param>
    /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
    /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
    public bool TryGetSetting<T>(string key, out T? value)
    {
        if (StageSettings == null)
        {
            value = default(T);
            return false;
        }
        else
        {
            return StageSettings.TryGetSetting(key, out value);
        }
    }

    /// <summary>
    /// Tries to get a setting with the specified type from the stage settings.
    /// </summary>
    /// <param name="key">The name of the setting..</param>
    /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
    /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
    public bool TryGetSetting(string key, [MaybeNullWhen(false)] out string value)
    {
        if (StageSettings == null)
        {
            value = null;
            return false;
        }
        else
        {
            return StageSettings.TryGetSetting(key, out value);
        }
    }

    /// <summary>
    /// Gets a string setting with the specified default value.
    /// </summary>
    /// <param name="key">The name of the setting.</param>
    /// <param name="defaultValue">The value to use if the setting is not present in the <see cref="SettingsDictionary"/>.</param>
    /// <returns>The value of the setting, or <paramref name="defaultValue"/> if the setting was not present in the <see cref="SettingsDictionary"/>.</returns>
    [return: NotNullIfNotNull(nameof(defaultValue))]
    public string? GetSetting(string key, string? defaultValue)
    {
        if (StageSettings == null)
        {
            return defaultValue;
        }
        else
        {
            return StageSettings.GetSetting(key, defaultValue);
        }
    }

    /// <summary>
    /// Adds a setting.
    /// </summary>
    /// <param name="key">The name of the setting.</param>
    /// <param name="value">The value of the setting.</param>
    public void AddSetting(string key, object value)
    {
        if (StageSettings == null)
        {
            StageSettings = new SettingsDictionary();
        }

        StageSettings.AddSetting(key, value);
    }

    /// <summary>
    /// Adds the specified settings.
    /// </summary>
    /// <param name="settings">The settings. May be <see langword="null"/>.</param>
    public void AddSettings(IEnumerable<KeyValuePair<string, string>> settings)
    {
        if (settings != null)
        {
            if (StageSettings == null)
            {
                StageSettings = new SettingsDictionary();
            }

            foreach (var setting in settings)
            {
                StageSettings.Add(setting.Key, setting.Value);
            }
        }
    }

    /// <summary>
    /// Checks whether this stage configuration is complete and consistent.
    /// </summary>
    /// <param name="job">The job that this stage belongs to.</param>
    /// <remarks>
    /// <para>
    ///   This method is intended to be used after constructing the job before it is submitted. It uses information that may not
    ///   be available after deserialization, and requires the various types to be loaded.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">The stage configuration is invalid.</exception>
    /// <exception cref="NotSupportedException">One of the record types used is not supported by <see cref="ValueWriter{T}"/>.</exception>
    public void Validate(JobConfiguration job)
    {
        ArgumentNullException.ThrowIfNull(job);
        // Some of the things checked here are also checked by the AddStage etc. methods of JobConfiguration, but almost all our properties are read/write (needed for XML serialization)
        // so it's possible to get the stage in an invalid state by modifying it after it has been added.
        if (string.IsNullOrWhiteSpace(StageId))
        {
            throw new InvalidOperationException("A stage cannot have a blank stage ID.");
        }

        if (TaskTypeInfo == null)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} must have a task type.", CompoundStageId));
        }

        if (TaskCount < 1)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} must have at least one task.", CompoundStageId));
        }
        // Not interested in the actual writers, but this method will throw an exception if the types aren't writable.
        ValueWriter.GetWriter(TaskTypeInfo.InputRecordType);
        ValueWriter.GetWriter(TaskTypeInfo.OutputRecordType);

        ValidateInput(job);
        ValidateOutput(job);

        if (DependentStages.Count > 0)
        {
            if (ChildStage != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have dependent stages because it has a child stage.", CompoundStageId));
            }

            foreach (var stageId in DependentStages)
            {
                if (job.GetStage(stageId) == null)
                {
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} specifies non-existant dependent stage ID {1}.", CompoundStageId, stageId));
                }
            }
        }
    }

    private void ValidateOutput(JobConfiguration job)
    {
        if (DataOutput != null)
        {
            if (ChildStage != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have data output because it has a child stage.", CompoundStageId));
            }

            if (OutputChannel != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} has both data output and an output channel.", CompoundStageId));
            }

            if (DataOutput.RecordType != TaskTypeInfo!.OutputRecordType)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output record type {1} is incompatible with its data output's record record type {2}.", StageId, DataOutput.RecordType, TaskTypeInfo.OutputRecordType));
            }

            if (!DataOutputType.TryGetReferencedType(out var type) || !type.IsInstanceOfType(DataOutput))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s data output type must match the data output instance.", CompoundStageId));
            }
        }
        else if (DataOutputType.TypeName != null)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s data output type must be null when the stage has no data output.", CompoundStageId));
        }

        if (OutputChannel != null)
        {
            if (ChildStage != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have both a child stage and an output channel.", CompoundStageId));
            }

            if (!OutputChannel.MultiInputRecordReaderType.TryGetReferencedType(out var multiInputRecordReaderType))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output channel must specify a multi-input record reader type.", CompoundStageId));
            }

            if (!OutputChannel.PartitionerType.TryGetReferencedType(out var partitionerType))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output channel must specify a partitioner type.", CompoundStageId));
            }

            ValidatePartitionerType(partitionerType);
            if (!MultiInputRecordReader.GetAcceptedInputTypes(multiInputRecordReaderType).Contains(TaskTypeInfo!.OutputRecordType))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output channel multi-input record reader type {1} doesn't accept the stage's output record type {2}.", CompoundStageId, multiInputRecordReaderType, TaskTypeInfo.OutputRecordType));
            }

            if (OutputChannel.OutputStage != null) // null is allowed for debugging purposes; see OutputChannel class
            {
                var receiver = job.GetStage(OutputChannel.OutputStage)
                    ?? throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output channel specifies non-existant output stage ID {1}.", CompoundStageId, OutputChannel.OutputStage));
                // Receiver types validated when the receiver's Validate method is called.
            }
        }

        if (ChildStage != null)
        {
            if (ChildStage.TaskCount > 1 && InternalPartitionCount > 1)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have a child stage with internal partitioning because internal partitioning was already applied in this compound stage.", CompoundStageId));
            }

            if (ChildStage.TaskCount > 1)
            {
                if (!ChildStagePartitionerType.TryGetReferencedType(out var childStagePartitionerType))
                {
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} must have a child stage partitioner because it has a child stage with internal partitioning.", CompoundStageId));
                }

                ValidatePartitionerType(childStagePartitionerType);
            }

            ChildStage.Validate(job);
            if (TaskTypeInfo!.OutputRecordType != ChildStage.TaskTypeInfo!.InputRecordType)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output record type {1} does not match its child stage's input record type {2}.", CompoundStageId, TaskTypeInfo.OutputRecordType, ChildStage.TaskTypeInfo.InputRecordType));
            }
        }
    }

    private void ValidateInput(JobConfiguration job)
    {
        if (DataInput != null)
        {
            if (Parent != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have data input because it is a child stage.", CompoundStageId));
            }

            if (job.GetInputStagesForStage(StageId!).Count() != 0)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} cannot have both data input and an input channel.", CompoundStageId));
            }

            if (DataInput.RecordType != TaskTypeInfo!.InputRecordType)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s input record type {1} is incompatible with its data input's record record type {2}.", CompoundStageId, DataInput.RecordType, TaskTypeInfo.InputRecordType));
            }

            if (!DataInputType.TryGetReferencedType(out var type) || !type.IsInstanceOfType(DataInput))
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s data input type must match the data input instance.", CompoundStageId));
            }
        }
        else
        {
            if (DataInputType.TypeName != null)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s data input type must be null when the stage has no data input.", CompoundStageId));
            }

            if (Parent == null)
            {
                var sendingStages = job.GetInputStagesForStage(StageId!).ToArray();
                IEnumerable<Type> inputTypes;
                if (sendingStages.Length > 1)
                {
                    if (!MultiInputRecordReaderType.TryGetReferencedType(out var multiInputRecordReaderType))
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s must specify a stage multi-input record reader because it has more than one input channel.", CompoundStageId));
                    }

                    if (RecordReader.GetRecordType(multiInputRecordReaderType) != TaskTypeInfo!.InputRecordType)
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s input record type {1} is not compatible with the stage multi-input record reader's record type {2}.", CompoundStageId, TaskTypeInfo.InputRecordType, RecordReader.GetRecordType(multiInputRecordReaderType)));
                    }

                    inputTypes = MultiInputRecordReader.GetAcceptedInputTypes(multiInputRecordReaderType);
                }
                else
                {
                    inputTypes = [TaskTypeInfo!.InputRecordType];
                }

                foreach (var sendingStage in sendingStages)
                {
                    if (!sendingStage.OutputChannel!.MultiInputRecordReaderType.TryGetReferencedType(out var multiInputRecordReaderType))
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output channel must specify a multi-input record reader type.", sendingStage.CompoundStageId));
                    }

                    if (!inputTypes.Contains(RecordReader.GetRecordType(multiInputRecordReaderType)))
                    {
                        throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s input channel uses incompatible record type {1}.", CompoundStageId, RecordReader.GetRecordType(multiInputRecordReaderType)));
                    }
                }
            }
        }
    }

    /// <summary>
    /// Returns a <see cref="System.String"/> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="System.String"/> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        return string.Format(CultureInfo.InvariantCulture, "StageConfiguration {{ StageId = \"{0}\" }}", StageId);
    }

    private static void ValidateInputType(IDataInput input, TaskTypeInfo taskType)
    {
        if (input.RecordType != taskType.InputRecordType)
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified input's record type {0} is not identical to the specified task type's input record type {1}.", input.RecordType, taskType.InputRecordType));
        }
    }

    private static void ValidateOutputType(IDataOutput output, TaskTypeInfo taskType)
    {
        if (output.RecordType != taskType.OutputRecordType)
        {
            throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified output's record type {0} is not identical to the specified task type's output record type {1}.", output.RecordType, taskType.OutputRecordType));
        }
    }

    private void ValidatePartitionerType(Type partitionerType)
    {
        if (partitionerType.ContainsGenericParameters)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s partitioner type must be a closed constructed generic type.", CompoundStageId));
        }

        var interfaceType = partitionerType.FindGenericInterfaceType(typeof(IPartitioner<>), false);
        if (interfaceType == null)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s partitioner type must implement IPartitioner<T>.", CompoundStageId));
        }

        var recordType = interfaceType.GetGenericArguments()[0];
        if (recordType != TaskTypeInfo?.OutputRecordType)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0}'s output record type {1} is incompatible with its partitioner's record type {2}.", CompoundStageId, TaskTypeInfo?.OutputRecordType, recordType));
        }
    }

}
