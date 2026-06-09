// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Reflection.Emit;
using Ookii.Jumbo.Jet.Jobs.Builder;

namespace Ookii.Jumbo.Jet.Jobs;

/// <summary>
/// Represents information about a type implementing <see cref="ITask{TInput,TOutput}"/>.
/// </summary>
public class TaskTypeInfo
{
    private readonly Type _inputRecordType;
    private readonly Type _outputRecordType;
    private readonly Type _taskType;
    private TaskRecordReuse? _recordReuse;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskTypeInfo"/> class.
    /// </summary>
    /// <param name="taskType">Type of the task.</param>
    /// <param name="input">The operation input, used to specialize single-parameter generic task types.</param>
    public TaskTypeInfo(Type taskType, IOperationInput? input = null)
    {
        ArgumentNullException.ThrowIfNull(taskType);
        if (taskType.IsGenericTypeDefinition)
        {
            if (input?.RecordType == null)
            {
                throw new ArgumentException("Task type must be a concrete type.", nameof(taskType));
            }

            taskType = taskType.MakeGenericType(input.RecordType);
        }

        if (taskType.ContainsGenericParameters)
        {
            throw new ArgumentException("The task must be closed constructed generic type.", nameof(taskType));
        }

        _taskType = taskType;
        var interfaceType = taskType.FindGenericInterfaceType(typeof(ITask<,>));
        var arguments = interfaceType.GetGenericArguments();
        _inputRecordType = arguments[0];
        _outputRecordType = arguments[1];
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskTypeInfo"/> class based on a dynamically generated task type.
    /// </summary>
    /// <param name="taskType">Type of the task.</param>
    /// <param name="inputRecordType">Type of the input records.</param>
    /// <param name="outputRecordType">Type of the output records.</param>
    /// <param name="recordReuse">Indicates whether the task type allows record reuse.</param>
    /// <remarks>
    /// <para>
    ///   Dynamically generated task types, created by the <see cref="DynamicTaskBuilder"/> class, do not support
    ///   reflection so certain information must be provided directly.
    /// </para>
    /// </remarks>
    public TaskTypeInfo(TypeBuilder taskType, Type inputRecordType, Type outputRecordType, TaskRecordReuse recordReuse)
    {
        ArgumentNullException.ThrowIfNull(taskType);
        _taskType = taskType;
        if (taskType.ContainsGenericParameters)
        {
            throw new ArgumentException("The task must be closed constructed generic type.", nameof(taskType));
        }

        _taskType = taskType.CreateType();
        _inputRecordType = inputRecordType;
        _outputRecordType = outputRecordType;
        _recordReuse = recordReuse;
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
    /// Gets the name of the task type.
    /// </summary>
    /// <value>
    /// The assembly qualified name of the task type.
    /// </value>
    public string TaskTypeName
    {
        get
        {
            // AssemblyQualifiedName doesn't work on dyanmic types.
            if (_taskType is TypeBuilder)
            {
                return _taskType.FullName + ", " + _taskType.Assembly.FullName;
            }

            return _taskType.AssemblyQualifiedName!;
        }
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
    public TaskRecordReuse RecordReuse => _recordReuse ??= DetermineRecordReuse();

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

    private TaskRecordReuse DetermineRecordReuse()
    {
        var recordReuseAttribute = (AllowRecordReuseAttribute?)Attribute.GetCustomAttribute(_taskType, typeof(AllowRecordReuseAttribute));
        if (recordReuseAttribute != null)
        {
            return recordReuseAttribute.PassThrough ? TaskRecordReuse.PassThrough : TaskRecordReuse.Allowed;
        }

        return TaskRecordReuse.NotAllowed;
    }
}
