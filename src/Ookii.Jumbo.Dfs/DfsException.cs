// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Represents an error with the distributed file system.
/// </summary>
public class DfsException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DfsException"/> class. 
    /// </summary>
    public DfsException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="DfsException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public DfsException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="DfsException"/> class with a specified error message and a reference to the inner <see cref="DfsException"/> that is the cause of this <see cref="DfsException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="DfsException"/>.</param>
    /// <param name="inner">The <see cref="DfsException"/> that is the cause of the current <see cref="DfsException"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="DfsException"/> is specified.</param>
    public DfsException(string message, Exception inner) : base(message, inner) { }
}
