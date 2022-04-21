// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Represents information about a type implementing <see cref="ITask{TInput,TOutput}"/>.
    /// </summary>
    public class TaskTypeInfo
    {
        private readonly Type _inputRecordType;
        private readonly Type _outputRecordType;
        private readonly Type _taskType;
        private readonly TaskRecordReuse _recordReuse;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskTypeInfo"/> class.
        /// </summary>
        /// <param name="taskType">Type of the task.</param>
        public TaskTypeInfo(Type taskType)
        {
            if (taskType == null)
                throw new ArgumentNullException(nameof(taskType));
            if (taskType.ContainsGenericParameters)
                throw new ArgumentException("The task must be closed constructed generic type.", nameof(taskType));

            _taskType = taskType;
            Type interfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>));
            Type[] arguments = interfaceType.GetGenericArguments();
            _inputRecordType = arguments[0];
            _outputRecordType = arguments[1];
            AllowRecordReuseAttribute recordReuseAttribute = (AllowRecordReuseAttribute)Attribute.GetCustomAttribute(taskType, typeof(AllowRecordReuseAttribute));
            if (recordReuseAttribute != null)
                _recordReuse = recordReuseAttribute.PassThrough ? TaskRecordReuse.PassThrough : TaskRecordReuse.Allowed;
        }

        /// <summary>
        /// Gets the type of the task.
        /// </summary>
        /// <value>
        /// The type of the task.
        /// </value>
        public Type TaskType
        {
            get { return _taskType; }
        }

        /// <summary>
        /// Gets the type of the input records.
        /// </summary>
        /// <value>
        /// The type of the input records.
        /// </value>
        public Type InputRecordType
        {
            get { return _inputRecordType; }
        }

        /// <summary>
        /// Gets the type of the output records.
        /// </summary>
        /// <value>
        /// The type of the output records.
        /// </value>
        public Type OutputRecordType
        {
            get { return _outputRecordType; }
        }

        /// <summary>
        /// Gets a value indicating whether the task type allows record reuse.
        /// </summary>
        /// <value>
        /// One of the values of the <see cref="TaskRecordReuse"/> enumeration.
        /// </value>
        public TaskRecordReuse RecordReuse
        {
            get { return _recordReuse; }
        }

        /// <summary>
        /// Gets a value indicating whether this task is a push task.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this task is a push task; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsPushTask
        {
            get { return _taskType.FindGenericBaseType(typeof(PushTask<,>), false) != null || _taskType.FindGenericBaseType(typeof(PrepartitionedPushTask<,>), false) != null; }
        }
    }
}
