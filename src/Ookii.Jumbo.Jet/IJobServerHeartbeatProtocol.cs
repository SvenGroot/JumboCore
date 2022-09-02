// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Protocol used by the task servers to communicate with the job server.
    /// </summary>
    public interface IJobServerHeartbeatProtocol
    {
        /// <summary>
        /// Sends a heartbeat to the name server.
        /// </summary>
        /// <param name="address">The <see cref="ServerAddress"/> of the server sending the heartbeat.</param>
        /// <param name="data">The data for the heartbeat.</param>
        /// <returns>An array of <see cref="JetHeartbeatResponse"/> for the heartbeat.</returns>
        JetHeartbeatResponse[] Heartbeat(ServerAddress address, JetHeartbeatData[] data);
    }
}
