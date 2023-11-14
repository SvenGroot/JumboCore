// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// Exception that is thrown when a <see cref="Packet"/>'s data does not match its checksum.
/// </summary>
public class InvalidPacketException : DfsException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPacketException"/> class. 
    /// </summary>
    public InvalidPacketException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPacketException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public InvalidPacketException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidPacketException"/> class with a specified error message and a reference to the inner <see cref="InvalidPacketException"/> that is the cause of this <see cref="InvalidPacketException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="InvalidPacketException"/>.</param>
    /// <param name="inner">The <see cref="InvalidPacketException"/> that is the cause of the current <see cref="InvalidPacketException"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="InvalidPacketException"/> is specified.</param>
    public InvalidPacketException(string message, Exception inner) : base(message, inner) { }
}
