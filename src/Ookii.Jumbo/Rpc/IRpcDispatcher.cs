using System.IO;

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Dispatches RPC requests to a server implementation.
/// </summary>
public interface IRpcDispatcher
{
    /// <summary>
    /// Dispatches an RPC request.
    /// </summary>
    /// <param name="operationName">The name of the operation to dispatch.</param>
    /// <param name="target">The object to invoke the operation on.</param>
    /// <param name="reader">A <see cref="BinaryReader"/> to read argument values from.</param>
    /// <param name="writer">A <see cref="BinaryWriter"/> to write the return value to.</param>
    void Dispatch(string operationName, object target, BinaryReader reader, BinaryWriter writer);
}
