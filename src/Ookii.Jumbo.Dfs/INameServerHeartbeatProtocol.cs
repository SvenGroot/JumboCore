// Copyright (c) Sven Groot (Ookii.org)

using Ookii.Jumbo.Rpc;

namespace Ookii.Jumbo.Dfs;

/// <summary>
/// The protocol used by the DataServers to send heartbeat messages to the DataServers.
/// </summary>
[RpcInterface]
public interface INameServerHeartbeatProtocol
{
    /// <summary>
    /// Sends a heartbeat to the name server.
    /// </summary>
    /// <param name="address">The <see cref="ServerAddress"/> of the server sending the heartbeat.</param>
    /// <param name="data">The data for the heartbeat.</param>
    /// <returns>An array of <see cref="HeartbeatResponse"/> for the heartbeat.</returns>
    /// <remarks>
    /// The <paramref name="address"/> parameter is necessary because data servers are identified by their
    /// host name and the port number they use to listen for clients, not their host name alone, so the
    /// name server cannot rely on <see cref="Ookii.Jumbo.Rpc.ServerContext.ClientHostName"/>.
    /// </remarks>
    HeartbeatResponse[]? Heartbeat(ServerAddress address, HeartbeatData[]? data);
}
