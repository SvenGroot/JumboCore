using System;
using System.Runtime.Serialization;

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Exception thrown when a remote operation threw an exception.
/// </summary>
[Serializable]
public class RpcRemoteException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class. 
    /// </summary>
    public RpcRemoteException(string originalExceptionType)
    {
        ArgumentNullException.ThrowIfNull(originalExceptionType);
        OriginalExceptionType = originalExceptionType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public RpcRemoteException(string? message, string originalExceptionType) : base(message)
    {
        ArgumentNullException.ThrowIfNull(originalExceptionType);
        OriginalExceptionType = originalExceptionType;
    }


    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class with a specified error message and a reference to the inner <see cref="RpcRemoteException"/> that is the cause of this <see cref="RpcRemoteException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="RpcRemoteException"/>.</param>
    /// <param name="inner">The <see cref="RpcRemoteException"/> that is the cause of the current <see cref="RpcRemoteException"/>, or a <see langword="null"/> if no inner <see cref="RpcRemoteException"/> is specified.</param>
    public RpcRemoteException(string? message, string originalExceptionType, Exception inner) : base(message, inner)
    {
        ArgumentNullException.ThrowIfNull(originalExceptionType);
        OriginalExceptionType = originalExceptionType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class with serialized data. 
    /// </summary>
    /// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the <see cref="RpcRemoteException"/> being thrown.</param>
    /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
    protected RpcRemoteException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
        OriginalExceptionType = info.GetString(nameof(OriginalExceptionType))!;
    }

    public string OriginalExceptionType { get; }

    /// <inheritdoc/>
    public override void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        base.GetObjectData(info, context);
        info.AddValue(nameof(OriginalExceptionType), OriginalExceptionType);
    }
}
