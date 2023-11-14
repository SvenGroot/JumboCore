// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Exception thrown if a error occurs reading from a child reader in a <see cref="MultiInputRecordReader{T}"/>.
/// </summary>
public class ChildReaderException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ChildReaderException"/> class. 
    /// </summary>
    public ChildReaderException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ChildReaderException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ChildReaderException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ChildReaderException"/> class with a specified error message and a reference to the inner <see cref="ChildReaderException"/> that is the cause of this <see cref="ChildReaderException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="ChildReaderException"/>.</param>
    /// <param name="inner">The <see cref="Exception"/> that is the cause of the current <see cref="Exception"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="Exception"/> is specified.</param>
    public ChildReaderException(string message, Exception inner) : base(message, inner) { }
}
