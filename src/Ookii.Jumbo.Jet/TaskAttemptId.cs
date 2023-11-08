// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Globalization;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// The identifier of a task attempt.
/// </summary>
[GeneratedValueWriter]
public sealed partial class TaskAttemptId : IEquatable<TaskAttemptId>, IComparable<TaskAttemptId>, IComparable
{
    /// <summary>
    /// The separator character used to identify the task attempt number in a task attempt identifier.
    /// </summary>
    public const char TaskAttemptNumberSeparator = '_';

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskAttemptId"/> class.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="attempt">The attempt number.</param>
    public TaskAttemptId(TaskId taskId, int attempt)
    {
        ArgumentNullException.ThrowIfNull(taskId);
        if (attempt <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(attempt), "The attempt number must be greater than zero.");
        }

        TaskId = taskId;
        Attempt = attempt;
    }

    /// <summary>
    /// Gets the task ID.
    /// </summary>
    /// <value>The task ID.</value>
    public TaskId TaskId { get; }

    /// <summary>
    /// Gets the attempt number
    /// </summary>
    /// <value>The attempt number.</value>
    public int Attempt { get; }

    /// <summary>
    /// Returns a <see cref="System.String"/> that represents this instance.
    /// </summary>
    /// <returns>
    /// A <see cref="System.String"/> that represents this instance.
    /// </returns>
    public override string ToString()
    {
        return TaskId.ToString() + TaskAttemptNumberSeparator + Attempt.ToString(CultureInfo.InvariantCulture);
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
    public override bool Equals(object? obj)
    {
        return Equals(obj as TaskAttemptId);
    }

    /// <summary>
    /// Returns a hash code for this instance.
    /// </summary>
    /// <returns>
    /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
    /// </returns>
    public override int GetHashCode() => HashCode.Combine(TaskId, Attempt);

    /// <summary>
    /// Compares this <see cref="TaskId"/> with another <see cref="TaskId"/>.
    /// </summary>
    /// <param name="other">The <see cref="TaskId"/> to compare to.</param>
    /// <returns><see langword="true"/> if this <see cref="TaskId"/> is equal to <paramref name="other"/>; otherwise, <see langword="false"/>.</returns>
    public bool Equals(TaskAttemptId? other)
    {
        if (other is null)
            return false;
        if (ReferenceEquals(other, this))
            return true;

        return TaskId.Equals(other.TaskId) && Attempt == other.Attempt;
    }

    /// <summary>
    /// Compares the current instance with another object of the same type and returns an integer that indicates whether the current instance precedes, follows, or occurs in the same position
    /// in the sort order as the other object.
    /// </summary>
    /// <param name="other">A <see cref="TaskId"/> to compare with this instance.</param>
    /// <returns>
    /// Less than zero if this instance is smaller than <paramref name="other"/>; zero if this instance is equal to <paramref name="other"/>; greater than zero if this instance is larger than <paramref name="other"/>.
    /// </returns>
    public int CompareTo(TaskAttemptId? other)
    {
        if (other is null)
            return 1;
        if (ReferenceEquals(other, this))
            return 0;

        var result = TaskId.CompareTo(other.TaskId);
        if (result == 0)
        {
            result = Attempt > other.Attempt ? 1 :
                (Attempt < other.Attempt ? -1 : 0);
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
    public int CompareTo(object? obj)
    {
        var other = obj as TaskAttemptId;
        if (other is null)
            throw new ArgumentException("obj is not a TaskAttemptId.", nameof(obj));
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
    public static bool operator ==(TaskAttemptId? left, TaskAttemptId? right)
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
    public static bool operator !=(TaskAttemptId? left, TaskAttemptId? right)
    {
        return !EqualityComparer<TaskAttemptId>.Default.Equals(left, right);
    }

}
