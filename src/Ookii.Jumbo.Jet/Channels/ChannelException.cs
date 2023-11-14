using System;

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// Exception that is thrown if an error occurs in the TCP or file channel.
/// </summary>
public class ChannelException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ChannelException"/> class.
    /// </summary>
    public ChannelException() { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ChannelException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public ChannelException(string message) : base(message) { }
    /// <summary>
    /// Initializes a new instance of the <see cref="ChannelException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="inner">The inner exception.</param>
    public ChannelException(string message, Exception inner) : base(message, inner) { }
}
