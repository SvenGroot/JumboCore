// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// The identifier of a task attempt.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1036:OverrideMethodsOnComparableTypes"), Serializable]
    public sealed class TaskAttemptId : IEquatable<TaskAttemptId>, IComparable<TaskAttemptId>, IComparable
    {
        private readonly TaskId _taskId;
        private readonly int _attempt;

        /// <summary>
        /// The separator characer used to identify the task attempt number in a task attempt identifier.
        /// </summary>
        public const char TaskAttemptNumberSeparator = '_';

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskAttemptId"/> class.
        /// </summary>
        /// <param name="taskId">The task ID.</param>
        /// <param name="attempt">The attempt number.</param>
        public TaskAttemptId(TaskId taskId, int attempt)
        {
            if( taskId == null )
                throw new ArgumentNullException("taskId");
            if( attempt <= 0 )
                throw new ArgumentOutOfRangeException("attempt", "The attempt number must be greater than zero.");

            _taskId = taskId;
            _attempt = attempt;
        }

        /// <summary>
        /// Gets the task ID.
        /// </summary>
        /// <value>The task ID.</value>
        public TaskId TaskId
        {
            get { return _taskId; }
        }

        /// <summary>
        /// Gets the attempt number
        /// </summary>
        /// <value>The attempt number.</value>
        public int Attempt
        {
            get { return _attempt; }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return _taskId.ToString() + TaskAttemptNumberSeparator + _attempt.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        /// 	<see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
        /// </returns>
        /// <exception cref="T:System.NullReferenceException">
        /// The <paramref name="obj"/> parameter is null.
        /// </exception>
        public override bool Equals(object obj)
        {
            return Equals(obj as TaskAttemptId);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _taskId.GetHashCode() ^ _attempt;
        }

        /// <summary>
        /// Compares this <see cref="TaskId"/> with another <see cref="TaskId"/>.
        /// </summary>
        /// <param name="other">The <see cref="TaskId"/> to compare to.</param>
        /// <returns><see langword="true"/> if this <see cref="TaskId"/> is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
        public bool Equals(TaskAttemptId other)
        {
            if( other == null )
                return false;
            if( other == this )
                return true;

            return _taskId.Equals(other._taskId) && _attempt == other._attempt;
        }

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that indicates whether the current instance precedes, follows, or occurs in the same position
        /// in the sort order as the other object.
        /// </summary>
        /// <param name="other">A <see cref="TaskId"/> to compare with this instance.</param>
        /// <returns>
        /// Less than zero if this instance is smaller than <paramref name="other"/>; zero if this instance is equal to <paramref name="other"/>; greater than zero if this instance is larger than <paramref name="other"/>.
        /// </returns>
        public int CompareTo(TaskAttemptId other)
        {
            if( other == null )
                return 1;
            if( other == this )
                return 0;

            int result = _taskId.CompareTo(other._taskId);
            if( result == 0 )
            {
                result = _attempt > other._attempt ? 1 :
                    (_attempt < other._attempt ? -1 : 0);
            }
            return result;
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
            TaskAttemptId other = obj as TaskAttemptId;
            if( other == null )
                throw new ArgumentException("obj is not a TaskAttemptId.", "obj");
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
        public static bool operator ==(TaskAttemptId left, TaskAttemptId right)
        {
            return EqualityComparer<TaskAttemptId>.Default.Equals(left, right);
        }

        /// <summary>
        /// Determines whether two specified instances have different values.
        /// </summary>
        /// <param name="left">The first instance to compare, or <see langword="null"/>.</param>
        /// <param name="right">The second instance to compare, or <see langword="null"/>.</param>
        /// <returns>
        /// <see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>. 
        /// </returns>
        public static bool operator !=(TaskAttemptId left, TaskAttemptId right)
        {
            return !EqualityComparer<TaskAttemptId>.Default.Equals(left, right);
        }
    
    }
}
