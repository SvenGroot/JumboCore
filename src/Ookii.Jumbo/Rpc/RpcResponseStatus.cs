// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Indicates the result of an RPC operation.
/// </summary>
public enum RpcResponseStatus
{
    /// <summary>
    /// The operation completed successfully, and produced a value which must be deserialized.
    /// </summary>
    Success,
    /// <summary>
    /// The operation completed successfully, and produced no value.
    /// </summary>
    SuccessNoValue,
    /// <summary>
    /// The operation encountered an error, and the remaining data is the serialized exception.
    /// </summary>
    Error
}
