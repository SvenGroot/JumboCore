using System;

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Indicates an interface will be used for Jumbo's RPC mechanism.
/// </summary>
/// <remarks>
/// <para>
///   When using this attribute, you must reference the Ookii.Jumbo.Generator assembly as an
///   analyzer. The source generator will build a client proxy type and a server dispatcher for
///   your interface.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Interface)]
public sealed class RpcInterfaceAttribute : Attribute
{
}
