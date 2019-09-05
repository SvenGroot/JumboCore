// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Ookii.Jumbo.Jet.Jobs.Builder
{
    /// <summary>
    /// An operation representing data processing being done in a single job stage.
    /// </summary>
    public class StageOperation : StageOperationBase
    {
        private readonly Channel _inputChannel;
        private readonly FileInput _dataInput;
        private readonly int _noInputTaskCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="StageOperation"/> class.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="input">The input for the operation.</param>
        /// <param name="taskType">Type of the task. May be a generic type definition with a single type parameter.</param>
        /// <remarks>
        /// If <paramref name="taskType"/> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
        /// You can use this with types such as <see cref="Tasks.EmptyTask{T}"/>, in which case you can specify them as <c>typeof(EmptyTask&lt;&gt;)</c> without
        /// specifying the record type.
        /// </remarks>
        public StageOperation(JobBuilder builder, IOperationInput input, Type taskType)
            : this(builder, input, 0, taskType)
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="StageOperation"/> class for a stage without input.
        /// </summary>
        /// <param name="builder">The job builder.</param>
        /// <param name="taskCount">The number of tasks in the stage.</param>
        /// <param name="taskType">Type of the task. May be a generic type definition with a single type parameter.</param>
        /// <remarks>
        /// If <paramref name="taskType"/> is a generic type definition with a singe type parameter, it will be constructed using the input's record type.
        /// You can use this with types such as <see cref="Tasks.EmptyTask{T}"/>, in which case you can specify them as <c>typeof(EmptyTask&lt;&gt;)</c> without
        /// specifying the record type.
        /// </remarks>
        public StageOperation(JobBuilder builder, int taskCount, Type taskType)
            : this(builder, null, taskCount, taskType)
        {
        }

        private StageOperation(JobBuilder builder, IOperationInput input, int noInputTaskCount, Type taskType)
            : base(builder, MakeGenericTaskType(taskType, input))
        {
            if( builder == null )
                throw new ArgumentNullException("builder");
            if( taskType == null )
                throw new ArgumentNullException("taskType");
            if( noInputTaskCount < 0 )
                throw new ArgumentOutOfRangeException("noInputTaskCount");
            if( noInputTaskCount == 0 && input == null )
                throw new ArgumentException("You must specify either an input or a task count larger than zero.");

            if( input != null )
            {
                if( TaskType.InputRecordType != input.RecordType )
                    throw new ArgumentException(string.Format(CultureInfo.CurrentCulture, "The input record type {0} of the task {1} doesn't match the record type {2} of the input.", TaskType.InputRecordType, taskType, input.RecordType));

                _dataInput = input as FileInput;
                if( _dataInput == null )
                    _inputChannel = new Channel((IJobBuilderOperation)input, this);
            }

            builder.AddOperation(this);
            builder.AddAssembly(taskType.Assembly);

            _noInputTaskCount = noInputTaskCount;
        }

        /// <summary>
        /// Gets the input channel for this operation.
        /// </summary>
        /// <value>
        /// The input channel, or <see langword="null"/>
        /// </value>
        public Channel InputChannel
        {
            get { return _inputChannel; }
        }

        /// <summary>
        /// Creates the configuration for this stage.
        /// </summary>
        /// <param name="compiler">The <see cref="JobBuilderCompiler"/>.</param>
        /// <returns>The <see cref="StageConfiguration"/> for the stage.</returns>
        protected override StageConfiguration CreateConfiguration(JobBuilderCompiler compiler)
        {
            if( compiler == null )
                throw new ArgumentNullException("compiler");
            if( _dataInput != null )
                return compiler.CreateStage(StageId, TaskType.TaskType, _dataInput, Output);
            else
                return compiler.CreateStage(StageId, TaskType.TaskType, _inputChannel == null ? _noInputTaskCount : _inputChannel.TaskCount, _inputChannel == null ? null : _inputChannel.CreateInput(), Output, true, _inputChannel == null ? null : _inputChannel.Settings);
        }

        private static Type MakeGenericTaskType(Type taskType, IOperationInput input)
        {
            // This only works for tasks with a single type argument (like EmptyTask<T>).
            if( taskType.IsGenericTypeDefinition && input != null )
                return taskType.MakeGenericType(input.RecordType);
            else
                return taskType;
        }
    }
}
