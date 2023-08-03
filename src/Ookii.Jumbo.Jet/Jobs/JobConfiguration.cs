// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Xml;
using System.Xml.Serialization;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;
using Ookii.Jumbo.Jet.IO;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides configuration information for a specific job.
    /// </summary>
    /// <seealso href="JobConfiguration.html">Job configuration XML documentation</seealso>
    [XmlRoot("Job", Namespace = JobConfiguration.XmlNamespace)]
    public class JobConfiguration
    {

        /// <summary>
        /// The XML namespace for the job configuration XML.
        /// </summary>
        public const string XmlNamespace = "http://www.ookii.org/schema/Jumbo/JobConfiguration";
        private static readonly XmlSerializer _serializer = new XmlSerializer(typeof(JobConfiguration));
        private readonly ExtendedCollection<string> _assemblyFileNames = new ExtendedCollection<string>();
        private readonly ExtendedCollection<StageConfiguration> _stages = new ExtendedCollection<StageConfiguration>();
        private readonly ExtendedCollection<AdditionalProgressCounter> _additionalProgressCounters = new ExtendedCollection<AdditionalProgressCounter>();
        private SchedulerOptions? _schedulerOptions;

        /// <summary>
        /// The key that can be used in the <see cref="JobSettings"/> or <see cref="StageConfiguration.StageSettings"/> to override the
        /// <see cref="JobServerConfigurationElement.SchedulingThreshold"/> setting. The value of this setting is a <see cref="Single"/>
        /// between 0 and 1 that indicates the scheduling threshold.
        /// </summary>
        public const string SchedulingThresholdSettingKey = "Jumbo.SchedulingThreshold";

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class.
        /// </summary>
        public JobConfiguration()
            : this((string[]?)null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly.
        /// </summary>
        /// <param name="assemblies">The assemblies containing the task types.</param>
        public JobConfiguration(params Assembly[] assemblies)
            : this(assemblies == null ? null : (from a in assemblies select System.IO.Path.GetFileName(a.Location)).ToArray())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JobConfiguration"/> class with the specified assembly file name.
        /// </summary>
        /// <param name="assemblyFileNames">The file names of the assemblies containing the task types for this class.</param>
        public JobConfiguration(params string[]? assemblyFileNames)
        {
            if (assemblyFileNames != null)
                _assemblyFileNames.AddRange(assemblyFileNames);
        }

        /// <summary>
        /// Gets or sets a descriptive name for the job. This is used for informational purposes only, and doesn't need to be unique.
        /// </summary>
        [XmlAttribute("name")]
        public string? JobName { get; set; }

        /// <summary>
        /// Gets the file name of the assembly holding the task classes.
        /// </summary>
        public Collection<string> AssemblyFileNames
        {
            get { return _assemblyFileNames; }
        }

        /// <summary>
        /// Gets a list of stages.
        /// </summary>  
        public Collection<StageConfiguration> Stages
        {
            get { return _stages; }
        }

        /// <summary>
        /// Gets the additional progress counters.
        /// </summary>
        /// <value>The additional progress counters.</value>
        public Collection<AdditionalProgressCounter> AdditionalProgressCounters
        {
            get { return _additionalProgressCounters; }
        }

        /// <summary>
        /// Gets or sets the options controlling the scheduler behavior.
        /// </summary>
        /// <value>The scheduler options.</value>
        public SchedulerOptions SchedulerOptions
        {
            get { return _schedulerOptions ?? (_schedulerOptions = new SchedulerOptions()); }
            set { _schedulerOptions = value; }
        }

        /// <summary>
        /// Gets a list of settings that can be accessed by the tasks in this job.
        /// </summary>
        public SettingsDictionary? JobSettings { get; set; }

        /// <summary>
        /// Adds a stage that reads input from a <see cref="IDataInput"/>.
        /// </summary>
        /// <param name="stageId">The name of the stage. This name will serve as the base name for all the tasks in the stage.</param>
        /// <param name="input">The <see cref="IDataInput"/> that provides the input.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        /// <remarks>
        ///   <note>
        /// Information about stages is not preserved through XML serialization, so you should not use this method on a <see cref="JobConfiguration"/>
        /// object created using the <see cref="LoadXml(string)"/> method.
        ///   </note>
        ///   <para>
        /// The new stage will contain as many tasks are there are blocks in the input file.
        ///   </para>
        /// </remarks>
        public StageConfiguration AddDataInputStage(string stageId, IDataInput input, Type taskType)
        {
            ArgumentNullException.ThrowIfNull(stageId);
            if (stageId.Length == 0)
                throw new ArgumentException("Stage ID cannot be empty.", nameof(stageId));
            ArgumentNullException.ThrowIfNull(input);
            ArgumentNullException.ThrowIfNull(taskType);

            var stage = CreateStage(stageId, taskType, 0, input);
            Stages.Add(stage);
            return stage;
        }

        /// <summary>
        /// Adds a stage that takes input from other stages or no input.
        /// </summary>
        /// <param name="stageId">The ID of the new stage.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks in the new stage.</param>
        /// <param name="inputStage">Information about the input stage for this stage, or <see langword="null"/> if the stage has no inputs.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        public StageConfiguration AddStage(string stageId, Type taskType, int taskCount, InputStageInfo? inputStage)
        {
            return AddStage(stageId, taskType, taskCount, inputStage == null ? null : new[] { inputStage }, null);
        }

        /// <summary>
        /// Adds a stage that takes input from other stages or no input.
        /// </summary>
        /// <param name="stageId">The ID of the new stage.</param>
        /// <param name="taskType">The type implementing the task's functionality; this type must implement <see cref="ITask{TInput,TOutput}"/>.</param>
        /// <param name="taskCount">The number of tasks in the new stage.</param>
        /// <param name="inputStages">Information about the input stages for this stage, or <see langword="null"/> if the stage has no inputs.</param>
        /// <param name="stageMultiInputRecordReaderType">The type of the multi input record reader to use to combine records from multiple input stages. This type must
        /// inherit from <see cref="MultiInputRecordReader{T}"/>. This type is not used if the stage has zero or one inputs.</param>
        /// <returns>
        /// A <see cref="StageConfiguration"/> for the new stage.
        /// </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "3")]
        public StageConfiguration AddStage(string stageId, Type taskType, int taskCount, IEnumerable<InputStageInfo>? inputStages, Type? stageMultiInputRecordReaderType)
        {
            ArgumentNullException.ThrowIfNull(stageId);
            ArgumentNullException.ThrowIfNull(taskType);
            if (taskCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(taskCount), "A stage must have at least one task.");

            var taskInterfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>), true)!;

            var inputType = taskInterfaceType.GetGenericArguments()[0];

            var isPipelineChannel = false;
            var hasInputs = false;
            if (inputStages != null)
            {
                if (inputStages.Count() > 1 && stageMultiInputRecordReaderType == null)
                    throw new ArgumentNullException(nameof(stageMultiInputRecordReaderType), "You must specify a stage multi input record reader if there is more than one input stage.");
                foreach (var info in inputStages)
                {
                    hasInputs = true;
                    if (info.ChannelType == ChannelType.Pipeline)
                    {
                        if (info.PartitionsPerTask > 1)
                            throw new ArgumentException("When using a pipeline channel, you cannot use multiple partitions per task.");
                        if (inputStages.Count() > 1)
                            throw new ArgumentException("When using a pipeline channel you can specify only one input.");
                        isPipelineChannel = true;
                    }
                    info.ValidateTypes(stageMultiInputRecordReaderType, inputType);
                }
            }

            var stage = CreateStage(stageId, taskType, taskCount, null);
            if (isPipelineChannel)
            {
                var parentStage = inputStages!.First();
                AddChildStage(parentStage.PartitionerType, inputType, stage, parentStage.InputStage);
            }
            else
            {
                if (hasInputs)
                {
                    Debug.Assert(inputStages != null);
                    if (inputStages.Count() > 1)
                    {
                        stage.MultiInputRecordReaderType = stageMultiInputRecordReaderType!;
                        AddAdditionalProgressCounter(stageMultiInputRecordReaderType!);
                    }

                    ValidateChannelConnectivityConstraints(inputStages, stage);

                    foreach (var info in inputStages)
                    {
                        if (info.InputStage.ChildStage != null)
                            throw new ArgumentException("One of the specified input stages already has a child stage so cannot be used as input.", nameof(inputStages));
                        else if (info.InputStage.HasDataOutput)
                            throw new ArgumentException("One of the specified input stages already has DFS output so cannot be used as input.", nameof(inputStages));
                        else if (info.InputStage.OutputChannel != null)
                            throw new ArgumentException("One of the specified input stages already has an output channel so cannot be used as input.", nameof(inputStages));
                    }

                    foreach (var info in inputStages)
                    {
                        var channel = new ChannelConfiguration()
                        {
                            ChannelType = info.ChannelType,
                            PartitionerType = info.PartitionerType,
                            MultiInputRecordReaderType = info.MultiInputRecordReaderType,
                            OutputStage = stageId,
                            PartitionsPerTask = info.PartitionsPerTask,
                            PartitionAssignmentMethod = info.PartitionAssignmentMethod,
                            DisableDynamicPartitionAssignment = info.DisableDynamicPartitionAssignment
                        };
                        info.InputStage.OutputChannel = channel;
                        AddAdditionalProgressCounter(info.ChannelType == ChannelType.Tcp ? typeof(TcpInputChannel) : typeof(FileInputChannel));
                        AddAdditionalProgressCounter(info.MultiInputRecordReaderType);
                    }
                }
                Stages.Add(stage);
            }
            return stage;

        }

        private static void ValidateChannelConnectivityConstraints(IEnumerable<InputStageInfo> inputStages, StageConfiguration stage)
        {
            foreach (var info in inputStages)
            {
                if (info.PartitionsPerTask > 1 && inputStages.Count() > 1)
                    throw new NotSupportedException("Using multiple partitions per task is not supported when using multiple input stages.");

                if (info.InputStage.InternalPartitionCount > 1 && info.InputStage.InternalPartitionCount != (stage.TaskCount * info.PartitionsPerTask))
                    throw new ArgumentException("A fully connected stage with an internally partitioned compound stage as input needs to have the same number of tasks as the input child stage.");
                break;
            }
        }

        private static void AddChildStage(Type partitionerType, Type inputType, StageConfiguration stage, StageConfiguration parentStage)
        {
            if (parentStage.ChildStage != null)
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because it already has a child stage.", stage.CompoundStageId));
            if (stage.TaskCount > 1 && parentStage.InternalPartitionCount > 1)
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage with internal partitioning to stage {0} because it already uses internal partitioning.", stage.CompoundStageId));
            if (stage.OutputChannel != null)
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because it already has an output channel.", stage.CompoundStageId));
            if (stage.DependentStages != null && stage.DependentStages.Count > 0)
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "Cannot add child stage to stage {0} because other stages have a scheduling dependency on it.", stage.CompoundStageId));
            parentStage.ChildStage = stage;
            parentStage.ChildStagePartitionerType = partitionerType ?? typeof(HashPartitioner<>).MakeGenericType(inputType);
        }

        private StageConfiguration CreateStage(string stageId, Type taskType, int taskCount, IDataInput? input)
        {
            var stage = new StageConfiguration()
            {
                StageId = stageId,
                TaskType = taskType,
                TaskCount = taskCount,
            };

            if (input != null)
            {
                // The property will validate the record types.
                stage.DataInput = input;
            }

            AddAdditionalProgressCounter(taskType);

            return stage;
        }

        /// <summary>
        /// Gets the root stage with the specified ID.
        /// </summary>
        /// <param name="stageId">The ID of the stage. This may not be a compound stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage, or <see langword="null"/> if no stage with that ID exists.</returns>
        public StageConfiguration? GetStage(string stageId)
        {
            return (from stage in Stages
                    where stage.StageId == stageId
                    select stage).SingleOrDefault();
        }

        /// <summary>
        /// Gets all stages in a compound stage ID.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>A list of all <see cref="StageConfiguration"/> instances for the stages, or <see langword="null"/> if any of the components
        /// of the compound stage ID could not be found.</returns>
        public IList<StageConfiguration>? GetPipelinedStages(string compoundStageId)
        {
            ArgumentNullException.ThrowIfNull(compoundStageId);

            var stageIds = compoundStageId.Split(TaskId.ChildStageSeparator);
            var stages = new List<StageConfiguration>(stageIds.Length);
            var current = GetStage(stageIds[0]);
            for (var x = 0; x < stageIds.Length; ++x)
            {
                if (x > 0)
                    current = current!.GetNamedChildStage(stageIds[x]);

                if (current == null)
                    return null;
                else
                    stages.Add(current);
            }
            return stages;
        }

        /// <summary>
        /// Gets the stage with the specified compound stage ID.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage, or <see langword="null"/> if no stage with that ID exists.</returns>
        public StageConfiguration? GetStageWithCompoundId(string compoundStageId)
        {
            ArgumentNullException.ThrowIfNull(compoundStageId);

            var stageIds = compoundStageId.Split(TaskId.ChildStageSeparator);
            var current = GetStage(stageIds[0]);
            for (var x = 0; x < stageIds.Length; ++x)
            {
                if (x > 0)
                    current = current!.GetNamedChildStage(stageIds[x]);

                if (current == null)
                    return null;
            }
            return current;
        }

        /// <summary>
        /// Gets the total number of tasks in a particular child stage.
        /// </summary>
        /// <param name="compoundStageId">The compound stage ID.</param>
        /// <returns>The number of tasks that will be created for the compound stage ID, which is the product of the number of tasks in each stage in the compound ID.</returns>
        public int GetTotalTaskCount(string compoundStageId)
        {
            var stages = GetPipelinedStages(compoundStageId);
            return GetTotalTaskCount(stages!, 0);
        }

        /// <summary>
        /// Gets the total number of tasks in a particular child stage.
        /// </summary>
        /// <param name="stages">A list of pipelined stages, as returned by <see cref="GetPipelinedStages"/>.</param>
        /// <param name="start">The index in <paramref name="stages"/> at which to start.</param>
        /// <returns>The number of tasks that will be created for the pipelined stages, which is the product of the number of tasks in each stage in the compound ID.</returns>
        public static int GetTotalTaskCount(IList<StageConfiguration> stages, int start)
        {
            ArgumentNullException.ThrowIfNull(stages);

            var result = 1;
            for (var x = start; x < stages.Count; ++x)
            {
                result *= stages[0].TaskCount;
            }
            return result;
        }

        /// <summary>
        /// Saves the current instance as XML to the specified stream.
        /// </summary>
        /// <param name="stream">The stream to save to.</param>
        public void SaveXml(System.IO.Stream stream)
        {
            ArgumentNullException.ThrowIfNull(stream);
            var settings = new XmlWriterSettings()
            {
                Indent = true,
                IndentChars = "  "
            };
            using (var writer = XmlWriter.Create(stream, settings))
            {
                writer.WriteStartDocument();
                writer.WriteProcessingInstruction("xml-stylesheet", "type=\"text/xsl\" href=\"config.xslt\"");
                _serializer.Serialize(writer, this);
            }
        }

        /// <summary>
        /// Gets the sending stages for the specified stage's input channel.
        /// </summary>
        /// <param name="stageId">The stage ID. This may not be a compound stage ID.</param>
        /// <returns>A list of stages whose <see cref="OutputChannel"/> is connected to the stage with the specified <paramref name="stageId"/>, or an empty list if the specified stage does not have an input channel or does not exist.</returns>
        public IEnumerable<StageConfiguration> GetInputStagesForStage(string stageId)
        {
            ArgumentNullException.ThrowIfNull(stageId);

            return from stage in Stages
                   let leaf = stage.Leaf
                   where leaf.OutputChannel != null && leaf.OutputChannel.OutputStage == stageId
                   select leaf;
        }

        /// <summary>
        /// Gets the stages that the specified stage explicitly depends on.
        /// </summary>
        /// <param name="stageId">The stage ID of the stage whose dependencies to retrieve.</param>
        /// <returns>A list of root stages that have the specified <paramref name="stageId"/> listed in their <see cref="StageConfiguration.Leaf"/> child stage's <see cref="StageConfiguration.DependentStages"/> collection.</returns>
        public IEnumerable<StageConfiguration> GetExplicitDependenciesForStage(string stageId)
        {
            ArgumentNullException.ThrowIfNull(stageId);

            return from stage in Stages
                   let leaf = stage.Leaf
                   where leaf.DependentStages.Contains(stageId)
                   select leaf;
        }

        /// <summary>
        /// Renames a stage and updates all references to its name.
        /// </summary>
        /// <param name="stage">The stage to rename.</param>
        /// <param name="newName">The new name of the stage.</param>
        public void RenameStage(StageConfiguration stage, string newName)
        {
            ArgumentNullException.ThrowIfNull(stage);
            ArgumentNullException.ThrowIfNull(newName);

            if (stage.Parent == null)
            {
                foreach (var dependency in GetExplicitDependenciesForStage(stage.StageId!))
                {
                    dependency.DependentStages.Remove(stage.StageId!);
                    dependency.DependentStages.Add(newName);
                }
                foreach (var inputStage in GetInputStagesForStage(stage.StageId!))
                {
                    inputStage.OutputChannel!.OutputStage = newName;
                }
            }
            stage.StageId = newName;
        }

        /// <summary>
        /// Gets all channels in the job.
        /// </summary>
        /// <returns>A list of all channels in the jobs.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<ChannelConfiguration> GetAllChannels()
        {
            var nestedStages = new Stack<StageConfiguration>(Stages);
            while (nestedStages.Count > 0)
            {
                // TODO: This code was adapted from when multiple child stages was still possibe, it might not be the best solution anymore.
                var stage = nestedStages.Pop();
                if (stage.ChildStage != null)
                {
                    nestedStages.Push(stage.ChildStage);
                }
                else if (stage.OutputChannel != null)
                    yield return stage.OutputChannel;
            }
        }

        /// <summary>
        /// Gets the top-level stages of the task in dependency order (if stage B depends on the output of stage A, then B will come after A in the order).
        /// </summary>
        /// <returns>The ordered list of stages.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        public IEnumerable<StageConfiguration> GetDependencyOrderedStages()
        {
            var result = new List<StageConfiguration>(Stages.Count);

            // Start with the stages that have no dependencies and no channel input.
            var dataInputStages = from stage in Stages
                                  where GetExplicitDependenciesForStage(stage.StageId!).Count() == 0 && GetInputStagesForStage(stage.StageId!).Count() == 0
                                  select stage;

            var nextStages = new Queue<StageConfiguration>(dataInputStages);

            while (nextStages.Count > 0)
            {
                var nextStage = nextStages.Dequeue();

                // If a stage has multiple input stages it might already be in the list. In that case we must remove it and re-add it at the end.
                result.Remove(nextStage);

                // A stage with a TCP channel as input must be scheduled before its sending stage, so it must be inserted into the list before the first of its inputs.
                var tcpChannelInputStageIndex = (from stage in GetInputStagesForStage(nextStage.StageId!)
                                                 where stage.OutputChannel!.ChannelType == ChannelType.Tcp
                                                 select result.IndexOf(stage.Root)).DefaultIfEmpty(-1).Min();

                if (tcpChannelInputStageIndex >= 0)
                    result.Insert(tcpChannelInputStageIndex, nextStage);
                else
                    result.Add(nextStage);

                nextStage = nextStage.Leaf;

                if (nextStage.OutputChannel != null)
                    nextStages.Enqueue(GetStage(nextStage.OutputChannel.OutputStage!)!);

                foreach (var stageId in nextStage.DependentStages)
                    nextStages.Enqueue(GetStage(stageId)!);
            }

            return result;
        }


        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="stream">The stream containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA5369:Use XmlReader For Deserialize", Justification = "It's actually safe.")]
        public static JobConfiguration LoadXml(System.IO.Stream stream)
        {
            ArgumentNullException.ThrowIfNull(stream);

            return (JobConfiguration)_serializer.Deserialize(stream)!;
        }

        /// <summary>
        /// Loads job configuration from an XML source.
        /// </summary>
        /// <param name="file">The path of the file containing the XML.</param>
        /// <returns>An instance of the <see cref="JobConfiguration"/> class created from the XML.</returns>
        public static JobConfiguration LoadXml(string file)
        {
            ArgumentNullException.ThrowIfNull(file);
            using (var stream = System.IO.File.OpenRead(file))
            {
                return LoadXml(stream);
            }
        }

        /// <summary>
        /// Adds an additional progress counter for the specified type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns><see langword="true"/> if the additional counter was added; <see langword="false"/> if <paramref name="type"/> was already added or didn't
        /// define an additional progress counter.</returns>
        public bool AddAdditionalProgressCounter(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);
            if (type.GetInterfaces().Contains(typeof(IHasAdditionalProgress)))
            {
                var counter = new AdditionalProgressCounter() { TypeName = type.FullName };
                // DisplayName isn't used in comparied AdditionalProgressCounter objects so we can postpone setting it.
                if (!_additionalProgressCounters.Contains(counter))
                {
                    var attribute = (AdditionalProgressCounterAttribute?)Attribute.GetCustomAttribute(type, typeof(AdditionalProgressCounterAttribute));
                    counter.DisplayName = attribute == null ? type.Name : attribute.DisplayName;
                    _additionalProgressCounters.Add(counter);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Checks whether this job configuration is complete and consistent.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   This method is intended to be used after constructing the job before it is submitted. It uses information that may not
        ///   be available after deserialization, and requires the various types to be loaded.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">The job configuration is invalid.</exception>
        /// <exception cref="NotSupportedException">One of the record types used is not supported by <see cref="ValueWriter{T}"/>.</exception>
        public void Validate()
        {
            if (Stages.Count == 0)
                throw new InvalidOperationException("The job has no stages.");
            var stageIds = new HashSet<string>();
            foreach (var stage in Stages)
            {
                stage.Validate(this);
                if (!stageIds.Add(stage.StageId!))
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "The job contains duplicate stage ID {0}.", stage.StageId));
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
            if (JobSettings == null)
                return defaultValue;
            else
                return JobSettings.GetSetting(key, defaultValue);
        }

        /// <summary>
        /// Tries to get a setting with the specified type from the job settings.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting..</param>
        /// <param name="value">If the function returns <see langword="true"/>, receives the value of the setting.</param>
        /// <returns><see langword="true"/> if the settings dictionary contained the specified setting; otherwise, <see langword="false"/>.</returns>
        public bool TryGetSetting<T>(string key, out T? value)
        {
            if (JobSettings == null)
            {
                value = default(T);
                return false;
            }
            else
                return JobSettings.TryGetSetting(key, out value);
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
            if (JobSettings == null)
                return defaultValue;
            else
                return JobSettings.GetSetting(key, defaultValue);
        }

        /// <summary>
        /// Adds a setting.
        /// </summary>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddSetting(string key, string value)
        {
            if (JobSettings == null)
                JobSettings = new SettingsDictionary();
            JobSettings.Add(key, value);
        }

        /// <summary>
        /// Adds a setting with the specified type.
        /// </summary>
        /// <typeparam name="T">The type of the setting.</typeparam>
        /// <param name="key">The name of the setting.</param>
        /// <param name="value">The value of the setting.</param>
        public void AddTypedSetting<T>(string key, T value)
            where T : notnull
        {
            AddSetting(key, value);
        }


        /// <summary>
        /// Adds the specified settings.
        /// </summary>
        /// <param name="settings">The settings. May be <see langword="null"/>.</param>
        public void AddSettings(IEnumerable<KeyValuePair<string, string>>? settings)
        {
            if (settings != null)
            {
                if (JobSettings == null)
                    JobSettings = new SettingsDictionary();

                foreach (var setting in settings)
                    JobSettings.Add(setting.Key, setting.Value);
            }
        }

        internal void AddSetting(string key, object value)
        {
            if (JobSettings == null)
                JobSettings = new SettingsDictionary();
            JobSettings.AddSetting(key, value);
        }
    }
}
