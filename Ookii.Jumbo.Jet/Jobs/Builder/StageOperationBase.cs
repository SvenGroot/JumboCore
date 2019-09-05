// $Id$

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// Base class for operations that create a stage.
    /// </summary>
    public abstract class StageOperationBase : IJobBuilderOperation
    {
        private readonly JobBuilder _builder;
        private readonly TaskTypeInfo _taskTypeInfo;

        private SettingsDictionary _settings;
        private List<StageOperationBase> _dependencies;
        private List<StageOperationBase> _dependentStages;

        private StageConfiguration _stage;

        private IOperationOutput _output;
        private string _stageId;

        /// <summary>
        /// Initializes a new instance of the <see cref="StageOperationBase"/> class.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="taskType">Type of the task.</param>
        protected StageOperationBase(JobBuilder builder, Type taskType)
        {
            if( builder == null )
                throw new ArgumentNullException("builder");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( taskType.IsGenericTypeDefinition )
                throw new ArgumentException("Task type must be a concrete type.", "taskType");

            _builder = builder;
            _taskTypeInfo = new TaskTypeInfo(taskType);
        }

        /// <summary>
        /// Gets or sets the name of the stage that will be created from this operation.
        /// </summary>
        /// <value>
        /// The name of the stage.
        /// </value>
        public string StageId
        {
            get { return _stageId ?? _taskTypeInfo.TaskType.Name + "Stage"; }
            set { _stageId = value; }
        }

        /// <summary>
        /// Gets information about the type of the task.
        /// </summary>
        /// <value>
        /// Information about the type of the task.
        /// </value>
        public TaskTypeInfo TaskType
        {
            get { return _taskTypeInfo; }
        }

        /// <summary>
        /// Gets the output for this operation.
        /// </summary>
        /// <value>
        /// The output, or <see langword="null"/> if no output has been specified.
        /// </value>
        protected IOperationOutput Output
        {
            get { return _output; }
        }

        /// <summary>
        /// Gets the settings for the stage.
        /// </summary>
        /// <value>
        /// A <see cref="SettingsDictionary"/> containing the settings.
        /// </value>
        public SettingsDictionary Settings
        {
            get { return _settings ?? (_settings = new SettingsDictionary()); }
        }

        /// <summary>
        /// Adds a scheduling dependency on the specified stage to this stage.
        /// </summary>
        /// <param name="stage">The stage that this stage depends on.</param>
        public void AddSchedulingDependency(StageOperation stage)
        {
            if( stage == null )
                throw new ArgumentNullException("stage");
            if( stage._builder != _builder )
                throw new ArgumentException("The specified stage does not belong to the same job.", "stage");

            // Dependencies are recorded in both directions so it doesn't matter which of the stages is created first by the JobBuilderCompiler.
            if( _dependencies == null )
                _dependencies = new List<StageOperationBase>();
            _dependencies.Add(stage);
            if( stage._dependentStages == null )
                stage._dependentStages = new List<StageOperationBase>();
            stage._dependentStages.Add(this);
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage.</returns>
        protected abstract StageConfiguration CreateConfiguration(JobBuilderCompiler compiler);

        private void ApplySchedulingDependencies()
        {
            if( _dependencies != null )
            {
                // We depend on other stages.
                if( _stage.Parent != null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} is a child stage which cannot have scheduler dependencies.", _stage.CompoundStageId));
                foreach( StageOperation stage in _dependencies )
                {
                    // If the stage config is null it hasn't been created yet, and the dependency will be set once it is created.
                    if( stage._stage != null )
                    {
                        if( stage._stage.ChildStage != null )
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Cannot add a dependency to stage {0} because it has a child stage.", stage._stage.CompoundStageId));
                        stage._stage.DependentStages.Add(_stage.StageId);
                    }
                }
            }

            if( _dependentStages != null )
            {
                // Other stages depend on us.
                if( _stage.ChildStage != null )
                    throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Cannot add a dependency to stage {0} because it has a child stage.", _stage.CompoundStageId));
                foreach( StageOperation stage in _dependentStages )
                {
                    // If the stage config is null it hasn't been created yet, and the dependency will be set once it is created.
                    if( stage._stage != null )
                    {
                        if( stage._stage.Parent != null )
                            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "Stage {0} is a child stage which cannot have scheduler dependencies.", stage._stage.CompoundStageId));
                        _stage.DependentStages.Add(stage._stage.StageId);
                    }
                }
            }
        }

        Type IOperationInput.RecordType
        {
            get { return _taskTypeInfo.OutputRecordType; }
        }

        StageConfiguration IJobBuilderOperation.Stage
        {
            get { return _stage; }
        }

        void IJobBuilderOperation.CreateConfiguration(JobBuilderCompiler compiler)
        {
            _stage = CreateConfiguration(compiler);
            _stage.AddSettings(_settings);
            ApplySchedulingDependencies();
        }

        void IJobBuilderOperation.SetOutput(IOperationOutput output)
        {
            if( output == null )
                throw new ArgumentNullException("output");
            if( _output != null )
                throw new InvalidOperationException("This operation already has an output.");
            _output = output;
        }

        JobBuilder IJobBuilderOperation.JobBuilder
        {
            get { return _builder; }
        }
    }
}
