// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Represents an error with the distributed file system.
/// </summary>
public class ServerShutdownException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ServerShutdownException"/> class. 
    /// </summary>
    public ServerShutdownException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ServerShutdownException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ServerShutdownException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ServerShutdownException"/> class with a specified error message and a reference to the inner <see cref="ServerShutdownException"/> that is the cause of this <see cref="ServerShutdownException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="ServerShutdownException"/>.</param>
    /// <param name="inner">The <see cref="ServerShutdownException"/> that is the cause of the current <see cref="ServerShutdownException"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="ServerShutdownException"/> is specified.</param>
    public ServerShutdownException(string message, Exception inner) : base(message, inner) { }
}
