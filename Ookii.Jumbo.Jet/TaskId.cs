// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Represents a task identifier.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1036:OverrideMethodsOnComparableTypes"), Serializable]
    public sealed class TaskId : ISerializable, IEquatable<TaskId>, IComparable<TaskId>, IComparable
    {
        private readonly string _taskId;
        private readonly string _stageId;
        private readonly int _taskNumber;
        private readonly TaskId _parentTaskId;

        /// <summary>
        /// The separator character used to identify child stages in a compound stage identifier, e.g. "Parent.Child".
        /// </summary>
        public const char ChildStageSeparator = '.';

        /// <summary>
        /// The separator character used to identify the task number in a task identifier, e.g. "StageId-204".
        /// </summary>
        public const char TaskNumberSeparator = '-';

        private static readonly char[] _invalidStageIdCharacters = { ChildStageSeparator, TaskNumberSeparator, TaskAttemptId.TaskAttemptNumberSeparator };

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified task ID.
        /// </summary>
        /// <param name="taskId">The string representation of the task ID. This can be a compound task ID.</param>
        public TaskId(string taskId)
            : this(null, taskId)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified parent task and task ID.
        /// </summary>
        /// <param name="parentTaskId">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="taskId">The string representation of the task ID. This can be a compound task ID
        /// only if <paramref name="parentTaskId"/> is <see langword="null"/>.</param>
        public TaskId(TaskId parentTaskId, string taskId)
        {
            if (taskId == null)
                throw new ArgumentNullException(nameof(taskId));

            if (parentTaskId != null)
            {
                if (taskId.Contains(ChildStageSeparator, StringComparison.Ordinal))
                    throw new ArgumentException("Task ID cannot contain a child stage separator ('.') if a parent task ID is specified.");
                _parentTaskId = parentTaskId;
                _taskId = parentTaskId.ToString() + ChildStageSeparator + taskId;
            }
            else
            {
                _taskId = taskId;
                var lastSeparatorIndex = taskId.LastIndexOf(ChildStageSeparator);
                if (lastSeparatorIndex >= 0)
                {
                    _parentTaskId = new TaskId(taskId.Substring(0, lastSeparatorIndex));
                    taskId = taskId.Substring(lastSeparatorIndex + 1);
                }
            }

            ParseStageIdAndNumber(taskId, out _stageId, out _taskNumber);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified parent task, stage ID and task number.
        /// </summary>
        /// <param name="parentTaskId">The ID of the the parent task; may be <see langword="null"/>.</param>
        /// <param name="stageId">The ID of the stage that this task belongs to.</param>
        /// <param name="taskNumber">The task number.</param>
        public TaskId(TaskId parentTaskId, string stageId, int taskNumber)
        {
            // CreateTaskIdString does the argument validation.
            var taskId = CreateTaskIdString(stageId, taskNumber);

            _stageId = stageId;
            _taskNumber = taskNumber;
            _parentTaskId = parentTaskId;

            if (parentTaskId != null)
                _taskId = parentTaskId.ToString() + ChildStageSeparator + taskId;
            else
                _taskId = taskId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskId"/> class with the specified stage ID and task number.
        /// </summary>
        /// <param name="stageId">The ID of the stage that this task belongs to.</param>
        /// <param name="taskNumber">The task number.</param>
        public TaskId(string stageId, int taskNumber)
            : this(null, stageId, taskNumber)
        {
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1801:Unused parameter", Justification = "Required parameter.")]
        private TaskId(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new ArgumentNullException(nameof(info));

            _taskId = info.GetString("TaskId");
            var localTaskId = _taskId;
            var lastSeparatorIndex = _taskId.LastIndexOf(ChildStageSeparator);
            if (lastSeparatorIndex >= 0)
            {
                _parentTaskId = new TaskId(_taskId.Substring(0, lastSeparatorIndex));
                localTaskId = _taskId.Substring(lastSeparatorIndex + 1);
            }

            ParseStageIdAndNumber(localTaskId, out _stageId, out _taskNumber);
        }

        /// <summary>
        /// Gets the ID of the parent task as a string.
        /// </summary>
        public TaskId ParentTaskId
        {
            get { return _parentTaskId; }
        }

        /// <summary>
        /// Gets the ID of the stage that this task belongs to.
        /// </summary>
        public string StageId
        {
            get { return _stageId; }
        }

        /// <summary>
        /// Gets the task number of this task.
        /// </summary>
        public int TaskNumber
        {
            get { return _taskNumber; }
        }

        /// <summary>
        /// Gets the compound stage ID of this task.
        /// </summary>
        public string CompoundStageId
        {
            get
            {
                var result = new StringBuilder(_taskId.Length);
                BuildCompoundStageId(result);
                return result.ToString();
            }
        }

        /// <summary>
        /// Gets the partition number of the task.
        /// </summary>
        /// <remarks>
        /// For non-child stages, this number is always 1. For child stages, this will find the location in the chain of parent stages
        /// in the compound where partitioning was done and return the task number of that task.
        /// </remarks>
        public int PartitionNumber
        {
            get
            {
                if (ParentTaskId == null)
                    return 1;
                else
                {
                    if (TaskNumber > 1)
                        return TaskNumber;
                    else
                        return ParentTaskId.PartitionNumber;
                }
            }
        }

        /// <summary>
        /// Gets a string representation of the <see cref="TaskId"/>.
        /// </summary>
        /// <returns>A string representation of the <see cref="TaskId"/>.</returns>
        public override string ToString()
        {
            return _taskId;
        }

        /// <summary>
        /// Creates a task ID string from the specified stage ID and task number.
        /// </summary>
        /// <param name="stageId">The stage ID.</param>
        /// <param name="taskNumber">The task number.</param>
        /// <returns>A task ID string.</returns>
        public static string CreateTaskIdString(string stageId, int taskNumber)
        {
            if (stageId == null)
                throw new ArgumentNullException(nameof(stageId));
            if (taskNumber < 0)
                throw new ArgumentOutOfRangeException(nameof(taskNumber), "Task number cannot be less than zero.");
            if (stageId.IndexOfAny(_invalidStageIdCharacters) >= 0)
                throw new ArgumentException("The characters '-', '.' and '_' may not occur in a stage ID.");

            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}{1}{2:000}", stageId, TaskNumberSeparator, taskNumber);
        }

        /// <summary>
        /// Populates a <see cref="System.Runtime.Serialization.SerializationInfo"/> with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo"/> to populate with data.</param>
        /// <param name="context">The destination (see <see cref="System.Runtime.Serialization.StreamingContext"/>) for this serialization.</param>
        /// <exception cref="System.Security.SecurityException">
        /// The caller does not have the required permission.
        /// </exception>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new ArgumentNullException(nameof(info));

            info.AddValue("TaskId", _taskId);
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        /// 	<see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
        /// </returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as TaskId);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _taskId.GetHashCode(StringComparison.Ordinal);
        }

        /// <summary>
        /// Compares this <see cref="TaskId"/> with another <see cref="TaskId"/>.
        /// </summary>
        /// <param name="other">The <see cref="TaskId"/> to compare to.</param>
        /// <returns><see langword="true"/> if this <see cref="TaskId"/> is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
        public bool Equals(TaskId other)
        {
            if (other == null)
                return false;
            if (other == this)
                return true;

            return string.Equals(_taskId, other._taskId, StringComparison.Ordinal);
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that indicates whether the current instance precedes, follows, or occurs in the same position
        /// in the sort order as the other object.
        /// </summary>
        /// <param name="other">A <see cref="TaskId"/> to compare with this instance.</param>
        /// <returns>
        /// Less than zero if this instance is smaller than <paramref name="other"/>; zero if this instance is equal to <paramref name="other"/>; greater than zero if this instance is larger than <paramref name="other"/>.
        /// </returns>
        public int CompareTo(TaskId other)
        {
            if (other == null)
                return 1;
            if (other == this)
                return 0;

            return string.CompareOrdinal(_taskId, other._taskId);
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that indicates whether the current instance precedes, follows, or occurs in the same position
        /// in the sort order as the other object.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>
        /// Less than zero if this instance is smaller than <paramref name="obj"/>; zero if this instance is equal to <paramref name="obj"/>; greater than zero if this instance is larger than <paramref name="obj"/>.
        /// </returns>
        public int CompareTo(object obj)
        {
            var other = obj as TaskId;
            if (other != null)
                throw new ArgumentException("The specified object is not a TaskId.", nameof(obj));

            return CompareTo(other);
        }

        /// <summary>
        /// Determines whether two specified instances have the same value.
        /// </summary>
        /// <param name="left">The first instance to compare, or <see langword="null"/>.</param>
        /// <param name="right">The second instance to compare, or <see langword="null"/>.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator ==(TaskId left, TaskId right)
        {
            return EqualityComparer<TaskId>.Default.Equals(left, right);
        }

        /// <summary>
        /// Determines whether two specified instances have different values.
        /// </summary>
        /// <param name="left">The first instance to compare, or <see langword="null"/>.</param>
        /// <param name="right">The second instance to compare, or <see langword="null"/>.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator !=(TaskId left, TaskId right)
        {
            return !EqualityComparer<TaskId>.Default.Equals(left, right);
        }

        private static void ParseStageIdAndNumber(string localTaskId, out string stageId, out int taskNumber)
        {
            var parts = localTaskId.Split(TaskNumberSeparator);
            if (parts.Length != 2)
                throw new FormatException("Task ID doesn't have the format StageId-Number.");
            stageId = parts[0];
            taskNumber = Convert.ToInt32(parts[1], System.Globalization.CultureInfo.InvariantCulture);
        }

        private void BuildCompoundStageId(StringBuilder result)
        {
            if (ParentTaskId != null)
            {
                var parent = ParentTaskId;
                parent.BuildCompoundStageId(result);
                result.Append(ChildStageSeparator);
            }
            result.Append(StageId);
        }
    }
}
