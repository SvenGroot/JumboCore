using System.IO;

namespace Ookii.Jumbo.Rpc;

public interface IRpcDispatcher
{
    void Dispatch(string operationName, object target, BinaryReader reader, BinaryWriter writer);
}
