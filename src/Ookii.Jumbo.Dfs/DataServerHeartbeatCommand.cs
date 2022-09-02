// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// The command given to a DataServer by the NameServer.
    /// </summary>
    public enum DataServerHeartbeatCommand
    {
        /// <summary>
        /// The name server doesn't have any tasks for the data server to perform.
        /// </summary>
        None,
        /// <summary>
        /// The name server wants the data server to send a full list of all its blocks.
        /// </summary>
        ReportBlocks,
        /// <summary>
        /// The name server wants the data server to delete certain blocks.
        /// </summary>
        /// <remarks>
        /// The most likely causes for this command would be if the data server reported blocks that the name server
        /// does not recognise, or if the some blocks are over-replicated.
        /// </remarks>
        DeleteBlocks,
        /// <summary>
        /// The name server wants the data server to replicate a block to a different data server.
        /// </summary>
        ReplicateBlock,
        /// <summary>
        /// The name server doesn't know this data server and needs an initial data heartbeat to verify the file system ID.
        /// </summary>
        SendInitialData
    }
}
