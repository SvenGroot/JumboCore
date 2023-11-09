﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Runtime.Serialization;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Exception thrown when a remote operation threw an exception.
/// </summary>
[Serializable]
public partial class RpcRemoteException : Exception
{
    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RpcRemoteException));

    // A handful of known exceptions whose type will be preserved across an RPC.
    private static readonly Dictionary<string, IValueWriter<Exception>> _writers = new()
    {
        { typeof(ArgumentException).AssemblyQualifiedName!, new MessageOnlyExceptionWriter<ArgumentException>() },
        { typeof(InvalidOperationException).AssemblyQualifiedName!, new MessageOnlyExceptionWriter<InvalidOperationException>() },
        { typeof(DirectoryNotFoundException).AssemblyQualifiedName!, new MessageOnlyExceptionWriter<DirectoryNotFoundException>() },
        { typeof(ArgumentNullException).AssemblyQualifiedName!, new ArgumentNullExceptionWriter() },
    };

    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class. 
    /// </summary>
    /// <param name="originalExceptionType">
    /// The full type name of the exception that was thrown by the remote server.
    /// </param>
    public RpcRemoteException(string originalExceptionType)
    {
        ArgumentNullException.ThrowIfNull(originalExceptionType);
        OriginalExceptionType = originalExceptionType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="originalExceptionType">
    /// The full type name of the exception that was thrown by the remote server.
    /// </param>
    public RpcRemoteException(string? message, string originalExceptionType) : base(message)
    {
        ArgumentNullException.ThrowIfNull(originalExceptionType);
        OriginalExceptionType = originalExceptionType;
    }


    /// <summary>
    /// Initializes a new instance of the <see cref="RpcRemoteException"/> class with a specified error message and a reference to the inner <see cref="RpcRemoteException"/> that is the cause of this <see cref="RpcRemoteException"/>. 
    /// </summary>
    /// <param name="message">The error message that explains the reason for the <see cref="RpcRemoteException"/>.</param>
    /// <param name="originalExceptionType">
    /// The full type name of the exception that was thrown by the remote server.
    /// </param>
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

    /// <summary>
    /// Gets the full type name of the exception that was thrown by the remote server.
    /// </summary>
    public string OriginalExceptionType { get; }

    /// <inheritdoc/>
    public override void GetObjectData(SerializationInfo info, StreamingContext context)
    {
        base.GetObjectData(info, context);
        info.AddValue(nameof(OriginalExceptionType), OriginalExceptionType);
    }

    internal static Exception ReadFrom(BinaryReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var originalExceptionType = reader.ReadString();
        var message = reader.ReadString();
        var stackTrace = reader.ReadString();
        var customSerializerSize = reader.Read7BitEncodedInt();
        Exception? ex = null;
        if (customSerializerSize > 0)
        {
            var data = reader.ReadBytes(customSerializerSize);
            using var innerStream = new MemoryStream(data);
            using var innerReader = new BinaryReader(innerStream);
            var writer = GetWriter(originalExceptionType);
            if (writer != null)
            {
                try
                {
                    ex = writer.Read(innerReader);
                }
                catch (Exception deserializeEx)
                {
                    _log.Error("Could not deserialize custom exception type.", deserializeEx);
                }
            }
        }

        ex ??= new RpcRemoteException(message, originalExceptionType);
        ExceptionDispatchInfo.SetRemoteStackTrace(ex, stackTrace);
        return ex;
    }

    internal static void WriteTo(Exception exception, BinaryWriter writer)
    {
        writer.Write((byte)RpcResponseStatus.Error);
        var name = exception.GetType().AssemblyQualifiedName ?? exception.GetType().Name;
        writer.Write(name);
        writer.Write(exception.Message);
        writer.Write(exception.StackTrace ?? "");
        var exWriter = GetWriter(name);
        byte[]? customSerializerData = null;
        if (exWriter != null)
        {
            using var innerStream = new MemoryStream();
            using var innerWriter = new BinaryWriter(innerStream);
            try
            {
                exWriter.Write(exception, innerWriter);
                customSerializerData = innerStream.ToArray();
            }
            catch (Exception deserializeEx)
            {
                _log.Error("Could not serialize custom exception type.", deserializeEx);
            }
        }

        writer.Write7BitEncodedInt(customSerializerData?.Length ?? 0);
        if (customSerializerData != null)
        {
            writer.Write(customSerializerData);
        }
    }

    private static IValueWriter<Exception>? GetWriter(string exceptionTypeName)
    {
        lock (_writers)
        {
            if (_writers.TryGetValue(exceptionTypeName, out var writer))
            {
                return writer;
            }
        }

        return null;
    }
}
