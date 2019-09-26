// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Rpc;
using Ookii.Jumbo.Topology;
using Ookii.Jumbo.Dfs.FileSystem;
using System.Globalization;

namespace NameServerApplication
{
    /// <summary>
    /// RPC server for the NameServer.
    /// </summary>
    public sealed class NameServer : MarshalByRefObject, INameServerClientProtocol, INameServerHeartbeatProtocol, IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(NameServer));

        private const int _checkpointInterval = 3600000; // 1 hour

        private readonly int _replicationFactor;
        private readonly int _blockSize;

        private readonly FileSystem _fileSystem;

        private readonly Dictionary<ServerAddress, DataServerInfo> _dataServers = new Dictionary<ServerAddress, DataServerInfo>();
        private readonly NetworkTopology _topology; // Lock _dataServers when accessing _topology; don't lock _topology itself.
        private readonly ReplicaPlacement _replicaPlacement; // Lock _dataServers when accessing _replicaPlacement.

        private readonly Dictionary<Guid, BlockInfo> _blocks = new Dictionary<Guid, BlockInfo>();
        private readonly Dictionary<Guid, PendingBlock> _pendingBlocks = new Dictionary<Guid, PendingBlock>();
        private readonly Dictionary<Guid, BlockInfo> _underReplicatedBlocks = new Dictionary<Guid, BlockInfo>();
        private readonly Thread _dataServerMonitorThread;
        private readonly AutoResetEvent _dataServerMonitorEvent = new AutoResetEvent(false);
        private bool _safeMode = true;
        private readonly Timer _checkpointTimer;
        private readonly ServerAddress _localAddress;
        private bool _running;


        private NameServer(JumboConfiguration jumboConfig, DfsConfiguration dfsConfig)
        {
            if( jumboConfig == null )
                throw new ArgumentNullException(nameof(jumboConfig));
            if( dfsConfig == null )
                throw new ArgumentNullException(nameof(dfsConfig));

            Configuration = dfsConfig;
            _localAddress = new ServerAddress(ServerContext.LocalHostName, dfsConfig.FileSystem.Url.Port);
            _topology = new NetworkTopology(jumboConfig);
            _replicaPlacement = new ReplicaPlacement(Configuration, _topology);
            _replicationFactor = dfsConfig.NameServer.ReplicationFactor;
            _blockSize = (int)dfsConfig.NameServer.BlockSize;
            _fileSystem = FileSystem.Load(dfsConfig);
            _fileSystem.FileDeleted += new EventHandler<FileDeletedEventArgs>(_fileSystem_FileDeleted);
            _fileSystem.GetBlocks(_blocks, _pendingBlocks);
            foreach( BlockInfo block in _blocks.Values )
            {
                _underReplicatedBlocks.Add(block.BlockId, block);
            }

            _dataServerMonitorThread = new Thread(DataServerMonitorThread)
            {
                Name = "DataServerMonitor",
                IsBackground = true,
            };
            _running = true;
            _dataServerMonitorThread.Start();
            _checkpointTimer = new Timer(CreateCheckpointTimerCallback, null, _checkpointInterval, _checkpointInterval);
        }

        public static NameServer Instance { get; private set; }

        public DfsConfiguration Configuration { get; private set; }

        public static void Run()
        {
            Run(JumboConfiguration.GetConfiguration(), DfsConfiguration.GetConfiguration());
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static void Run(JumboConfiguration jumboConfig, DfsConfiguration dfsConfig)
        {
            if( jumboConfig == null )
                throw new ArgumentNullException(nameof(jumboConfig));
            if( dfsConfig == null )
                throw new ArgumentNullException(nameof(dfsConfig));

            _log.Info("---- NameServer is starting ----");
            _log.LogEnvironmentInformation();
            
            Instance = new NameServer(jumboConfig, dfsConfig);
            ConfigureRemoting(dfsConfig);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static void Shutdown()
        {
            RpcHelper.UnregisterServerChannels(Instance.Configuration.FileSystem.Url.Port);
            Instance.ShutdownInternal();
            Instance.Dispose();
            Instance = null;
            _log.Info("---- NameServer has shut down ----");
        }

        public override object InitializeLifetimeService()
        {
            // This causes the object to live forever.
            return null;
        }

        public void CheckBlockReplication(IEnumerable<Guid> blocks)
        {
            if (blocks == null)
                throw new ArgumentNullException(nameof(blocks));

            // I believe this is what Hadoop does, but is it the right thing to do?
            lock( _underReplicatedBlocks )
            {
                foreach( Guid blockID in blocks )
                {
                    BlockInfo info;
                    if( _underReplicatedBlocks.TryGetValue(blockID, out info) )
                        throw new DfsException("Cannot add a block to a file with under-replicated blocks.");
                }
            }
        }


        private void DiscardBlock(Guid blockID)
        {
            lock( _pendingBlocks )
            {
                _pendingBlocks.Remove(blockID);
            }
            lock( _dataServers )
            {
                foreach( DataServerInfo server in _dataServers.Values )
                    server.PendingBlocks.Remove(blockID);
            }
        }

        private void CommitBlock(Guid blockID)
        {
            lock( _blocks )
            {
                lock( _pendingBlocks )
                {
                    PendingBlock pendingBlock;
                    if( _pendingBlocks.TryGetValue(blockID, out pendingBlock) )
                    {
                        // No need to remove the block from the data servers PendingBlocks list, because that has already been done when the data servers sent
                        // a new block heartbeat.
                        _pendingBlocks.Remove(blockID);
                        _blocks.Add(blockID, pendingBlock.Block);
                        // This can happen during log file replay or if a server crashed between commits.
                        if( pendingBlock.Block.DataServers.Count < pendingBlock.Block.File.ReplicationFactor )
                        {
                            lock( _underReplicatedBlocks )
                                _underReplicatedBlocks.Add(blockID, pendingBlock.Block);
                        }
                    }
                }
            }
        }

        #region IClientProtocol Members

        public int BlockSize
        {
            get 
            {
                _log.Debug("BlockSize called");
                return _blockSize; 
            }
        }

        public bool SafeMode
        {
            get 
            {
                _log.Debug("SafeMode called");
                return _safeMode; 
            }
            [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
            set
            {
                _log.DebugFormat("Setting SafeMode to {0}, current value {1}.", value, _safeMode);
                if( _safeMode != value )
                {
                    if( !value )
                    {
                        lock( _dataServers )
                        {
                            if( _dataServers.Count < _replicationFactor )
                                throw new InvalidOperationException("Safe mode cannot be disabled if there are insufficient data servers for full replication with the default replication factor.");
                        }
                    }
                    _safeMode = value;
                    if( _safeMode )
                    {
                        _log.Info("Safemode is ON.");
                    }
                    else
                    {
                        _log.Info("Safemode is OFF.");
                        _dataServerMonitorEvent.Set(); // Force an immediate replication check if safe mode is disabled prematurely.
                    }
                }
            }
        }

        public void CreateDirectory(string path)
        {
            _log.Debug("CreateDirectory called");
            CheckSafeMode();
            _fileSystem.CreateDirectory(path);
        }

        public JumboDirectory GetDirectoryInfo(string path)
        {
            _log.Debug("GetDirectoryInfo called");
            return _fileSystem.GetDirectoryInfo(path);
        }


        public BlockAssignment CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions)
        {
            _log.Debug("CreateFile called");
            CheckSafeMode();
            BlockInfo block = _fileSystem.CreateFile(path, blockSize == 0 ? BlockSize : blockSize, replicationFactor == 0 ? _replicationFactor : replicationFactor, recordOptions);
            try
            {
                lock( _pendingBlocks )
                {
                    _pendingBlocks.Add(block.BlockId, new PendingBlock(block));
                } 
                return AssignBlockToDataServers(block, useLocalReplica);
            }
            catch( Exception )
            {
                CloseFile(path);
                Delete(path, false);
                throw;
            }
        }

        public bool Delete(string path, bool recursive)
        {
            _log.Debug("Delete called");
            CheckSafeMode();
            return _fileSystem.Delete(path, recursive);
        }

        public void Move(string from, string to)
        {
            _log.Debug("Move called");
            CheckSafeMode();
            _fileSystem.Move(from, to);
        }

        public JumboFile GetFileInfo(string path)
        {
            _log.Debug("GetFileInfo called");
            return _fileSystem.GetFileInfo(path);
        }

        public JumboFileSystemEntry GetFileSystemEntryInfo(string path)
        {
            _log.Debug("GetFileSystemEntry called.");
            return _fileSystem.GetFileSystemEntryInfo(path);
        }

        public BlockAssignment AppendBlock(string path, bool useLocalReplica)
        {
            _log.Debug("AppendBlock called");
            CheckSafeMode();

            int dataServerCount;
            lock( _dataServers )
            {
                dataServerCount = _dataServers.Count;
            }

            BlockInfo block = _fileSystem.AppendBlock(path, dataServerCount);
            lock( _pendingBlocks )
            {
                _pendingBlocks.Add(block.BlockId, new PendingBlock(block));
            }

            return AssignBlockToDataServers(block, useLocalReplica);
        }

        public void CloseFile(string path)
        {
            _log.Debug("CloseFile called");
            CheckSafeMode();
            Guid? pendingBlock = _fileSystem.CloseFile(path);
            if( pendingBlock != null )
                DiscardBlock(pendingBlock.Value);
        }

        public ServerAddress[] GetDataServersForBlock(Guid blockID)
        {
            _log.Debug("GetDataServersForBlock called");
            // I allow calling this even if safemode is on, but it might return an empty list in that case.
            lock( _blocks )
            {
                lock( _dataServers )
                {
                    BlockInfo block;
                    if( !_blocks.TryGetValue(blockID, out block) )
                        throw new ArgumentException("Invalid block ID.");

                    string hostName = ServerContext.Current.ClientHostName;
                    string rackId = _topology.ResolveNode(hostName);

                    return (from server in block.DataServers
                            orderby server.DistanceFrom(hostName, rackId) ascending
                            select server.Address).ToArray();
                }
            }
        }

        public string GetFileForBlock(Guid blockId)
        {
            // This call is allowed even if safemode is on.
            lock( _blocks )
            {
                BlockInfo block;
                if( _blocks.TryGetValue(blockId, out block) )
                    return block.File.FullPath;
                else
                    return null;
            }
        }

        public Guid[] GetBlocks(BlockKind kind)
        {
            switch( kind )
            {
            case BlockKind.Normal:
                lock( _blocks )
                {
                    return _blocks.Keys.ToArray();
                }
            case BlockKind.Pending:
                lock( _pendingBlocks )
                {
                    return _pendingBlocks.Keys.ToArray();
                }
            case BlockKind.UnderReplicated:
                lock( _underReplicatedBlocks )
                {
                    return _underReplicatedBlocks.Keys.ToArray();
                }
            }

            throw new ArgumentException("Invalid block kind.", nameof(kind));
        }

        public DfsMetrics GetMetrics()
        {
            _log.Debug("GetMetrics called");
            DfsMetrics metrics = new DfsMetrics()
            {
                NameServer = _localAddress
            };

            lock( _blocks )
            {
                metrics.TotalBlockCount = _blocks.Count;
            }
            lock( _pendingBlocks )
            {
                metrics.PendingBlockCount = _pendingBlocks.Count;
            }
            lock( _underReplicatedBlocks )
            {
                metrics.UnderReplicatedBlockCount = _underReplicatedBlocks.Count;
            }
            lock( _dataServers )
            {
                foreach( DataServerInfo server in _dataServers.Values )
                {
                    metrics.DataServers.Add(new DataServerMetrics()
                    {
                        Address = server.Address,
                        RackId = server.Rack.RackId,
                        LastContactUtc = server.LastContactUtc,
                        BlockCount = server.Blocks.Count,
                        DiskSpaceFree = server.DiskSpaceFree,
                        DiskSpaceUsed = server.DiskSpaceUsed,
                        DiskSpaceTotal = server.DiskSpaceTotal
                    });
                }
            }
            metrics.TotalSize = _fileSystem.TotalSize;
            return metrics;
        }

        public int GetDataServerBlockCount(ServerAddress dataServer, Guid[] blocks)
        {
            _log.DebugFormat("GetDataServerBlockCount, dataServer = {0}", dataServer);
            if( dataServer == null )
                throw new ArgumentNullException(nameof(dataServer));
            if( blocks == null )
                throw new ArgumentNullException(nameof(blocks));
            lock( _dataServers )
            {
                DataServerInfo server;
                if( !_dataServers.TryGetValue(dataServer, out server) )
                {
                    server = (from s in _dataServers.Values
                              where s.Address.HostName == dataServer.HostName
                              select s).First();
                }
                return server.Blocks.Intersect(blocks).Count();
            }
        }

        public Guid[] GetDataServerBlocks(ServerAddress dataServer)
        {
            _log.DebugFormat("GetDataServerBlocks, dataServer = {0}", dataServer);
            if( dataServer == null )
                throw new ArgumentNullException(nameof(dataServer));

            lock( _dataServers )
            {
                DataServerInfo server = _dataServers[dataServer];
                return server.Blocks.ToArray();
            }
        }

        public Guid[] GetDataServerBlocksFromList(ServerAddress dataServer, Guid[] blocks)
        {
            _log.DebugFormat("GetDataServerBlockCount, dataServer = {0}", dataServer);
            if( dataServer == null )
                throw new ArgumentNullException(nameof(dataServer));
            if( blocks == null )
                throw new ArgumentNullException(nameof(blocks));
            lock( _dataServers )
            {
                DataServerInfo server = _dataServers[dataServer];
                return server.Blocks.Intersect(blocks).ToArray();
            }
        }

        public string GetLogFileContents(LogFileKind kind, int maxSize)
        {
            return LogFileHelper.GetLogFileContents("NameServer", kind, maxSize);
        }

        public void RemoveDataServer(ServerAddress dataServer)
        {
            if( dataServer == null )
                throw new ArgumentNullException(nameof(dataServer));

            DataServerInfo info;
            lock( _dataServers )
            {
                if( _dataServers.TryGetValue(dataServer, out info) )
                {
                    RemoveDataServer(info);
                    _dataServerMonitorEvent.Set();
                }
            }
        }

        public void CreateCheckpoint()
        {
            _log.Debug("CreateCheckpoint");
            _fileSystem.SaveToFileSystemImage();
        }

        #endregion

        #region INameServerHeartbeatProtocol Members

        public HeartbeatResponse[] Heartbeat(ServerAddress address, HeartbeatData[] data)
        {
            //_log.Debug("Data server heartbeat received.");
            if( address == null )
                throw new ArgumentNullException(nameof(address));

            DataServerInfo dataServer;
            List<HeartbeatResponse> responseList = null;
            lock( _dataServers )
            {
                InitialHeartbeatData initialData = data != null && data.Length > 0 ? data[0] as InitialHeartbeatData : null;
                bool serverKnown = _dataServers.TryGetValue(address, out dataServer);
                if( initialData != null || !serverKnown )
                {
                    if( serverKnown && initialData != null )
                    {
                        _log.WarnFormat("Data server {0} sent initial contact data but was already known; deleting previous data.", address);
                        RemoveDataServer(dataServer);
                    }
                    else if( initialData == null )
                    {
                        _log.WarnFormat("A new data server has reported in at {0} but didn't send initial data.", address);
                        // Tell the server to send initial data. Don't add it yet because we don't know if it belongs to this file system.
                        return new[] { new HeartbeatResponse(_fileSystem.FileSystemId, DataServerHeartbeatCommand.SendInitialData) };
                    }
                    else
                        _log.InfoFormat("A new data server has reported in at {0}.", address);

                    if( initialData.FileSystemId != Guid.Empty && initialData.FileSystemId != _fileSystem.FileSystemId )
                    {
                        _log.WarnFormat("Data server {0} has incorrect file system ID {1:B}.", address, initialData.FileSystemId);
                        throw new ArgumentException("Invalid file system ID."); // Will make the data server crash, which is good.
                    }

                    if( address.HostName != ServerContext.Current.ClientHostName )
                        _log.Warn("The data server reported a different hostname than is indicated in the ServerContext.");
                    dataServer = new DataServerInfo(address, _fileSystem.FileSystemId);
                    _topology.AddNode(dataServer);
                    _dataServers.Add(address, dataServer);
                }

                dataServer.LastContactUtc = DateTime.UtcNow;

                if( data != null )
                {
                    foreach( HeartbeatData item in data )
                    {
                        HeartbeatResponse response = ProcessHeartbeat(item, dataServer);
                        if( response != null )
                        {
                            if( responseList == null )
                                responseList = new List<HeartbeatResponse>();
                            responseList.Add(response);
                        }
                    }
                }

                if( !dataServer.HasReportedBlocks )
                {
                    Debug.Assert(responseList == null);
                    return new[] { new HeartbeatResponse(_fileSystem.FileSystemId, DataServerHeartbeatCommand.ReportBlocks) };
                }

                HeartbeatResponse[] pendingResponses = dataServer.GetAndClearPendingResponses();
                if( pendingResponses.Length > 0 )
                {
                    if( responseList == null )
                        responseList = new List<HeartbeatResponse>();
                    responseList.AddRange(pendingResponses);
                }
            }

            return responseList == null ? null : responseList.ToArray();
        }

        #endregion

        void _fileSystem_FileDeleted(object sender, FileDeletedEventArgs e)
        {
            if( e.PendingBlock != null )
            {
                lock( _pendingBlocks )
                {
                    PendingBlock info;
                    if( _pendingBlocks.TryGetValue(e.PendingBlock.Value, out info) )
                    {
                        _pendingBlocks.Remove(e.PendingBlock.Value);
                        MarkForDataServerDeletion(e.PendingBlock.Value, info.Block);
                    }
                    else
                        _log.Warn("File system attempted to delete a file with unknown pending block.");
                }
                lock( _dataServers )
                {
                    foreach( DataServerInfo server in _dataServers.Values )
                        server.PendingBlocks.Remove(e.PendingBlock.Value);
                }
            }
            if( e.File.Blocks.Count > 0 )
            {
                List<BlockInfo> removedBlocks = new List<BlockInfo>(e.File.Blocks.Count);
                lock( _blocks )
                {
                    lock( _underReplicatedBlocks )
                    {
                        foreach( var block in e.File.Blocks )
                        {
                            BlockInfo info;
                            if( _blocks.TryGetValue(block, out info) )
                            {
                                _blocks.Remove(block);
                                _underReplicatedBlocks.Remove(block);
                                MarkForDataServerDeletion(block, info);
                                removedBlocks.Add(info);
                            }
                            else
                                _log.Warn("File system attempted to delete a file with unknown blocks.");
                        }
                    }
                }
                lock( _dataServers )
                {
                    foreach( BlockInfo block in removedBlocks )
                    {
                        foreach( DataServerInfo server in block.DataServers )
                        {
                            server.Blocks.Remove(block.BlockId);
                        }
                    }
                }
            }
        }

        private void ShutdownInternal()
        {
            using( ManualResetEvent evt = new ManualResetEvent(false) )
            {
                _checkpointTimer.Dispose(evt);
                evt.WaitOne();
            }

            _running = false;
            _dataServerMonitorEvent.Set();
            _dataServerMonitorThread.Join();

            _fileSystem.Dispose();
        }

        private HeartbeatResponse ProcessHeartbeat(HeartbeatData data, DataServerInfo dataServer)
        {
            StatusHeartbeatData status = data as StatusHeartbeatData;
            if( status != null )
            {
                _log.InfoFormat(CultureInfo.InvariantCulture, "Data server {0} status: {1}B used, {2}B free, {3}B total.", dataServer.Address, status.DiskSpaceUsed, status.DiskSpaceFree, status.DiskSpaceTotal);
                dataServer.DiskSpaceFree = status.DiskSpaceFree;
                dataServer.DiskSpaceUsed = status.DiskSpaceUsed;
                dataServer.DiskSpaceTotal = status.DiskSpaceTotal;
                // Don't return; some of the other heartbeat types inherit from StatusHeartbeatData
            }

            BlockReportHeartbeatData blockReport = data as BlockReportHeartbeatData;
            if( blockReport != null )
            {
                return ProcessBlockReport(dataServer, blockReport);
            }

            NewBlockHeartbeatData newBlock = data as NewBlockHeartbeatData;
            if( newBlock != null )
            {
                return ProcessNewBlock(dataServer, newBlock);
            }

            return null;
        }

        private HeartbeatResponse ProcessNewBlock(DataServerInfo dataServer, NewBlockHeartbeatData newBlock)
        {
            _log.InfoFormat("Data server {2} reports it has received block {0} of size {1}.", newBlock.BlockId, newBlock.Size, dataServer.Address);

            if( dataServer.Blocks.Contains(newBlock.BlockId) )
            {
                _log.WarnFormat("Data server {0} already had block {1}.", dataServer.Address, newBlock.BlockId);
                return null;
            }

            bool commitBlock = false;
            HeartbeatResponse response = null;
            PendingBlock pendingBlock;
            lock( _pendingBlocks )
            {
                if( _pendingBlocks.TryGetValue(newBlock.BlockId, out pendingBlock) )
                {
                    // TODO: Should there be some kind of check whether the data server reporting this was actually
                    // one of the assigned servers?
                    pendingBlock.Block.DataServers.Add(dataServer);
                    dataServer.Blocks.Add(newBlock.BlockId);
                    dataServer.PendingBlocks.Remove(newBlock.BlockId);
                    if( pendingBlock.IncrementCommit() >= pendingBlock.Block.File.ReplicationFactor )
                    {
                        commitBlock = true;
                    }
                }
                // We don't need to check in _blocks; they're not moved there until all data servers have been checked in.
            }
            if( commitBlock )
            {
                _log.InfoFormat("Pending block {0} is now fully replicated and is being committed.", newBlock.BlockId);
                _fileSystem.CommitBlock(pendingBlock.Block.File.FullPath, newBlock.BlockId, newBlock.Size);
                CommitBlock(newBlock.BlockId);
            }
            else if( pendingBlock == null )
            {
                lock( _blocks )
                {
                    lock( _underReplicatedBlocks )
                    {
                        BlockInfo block;
                        if( _underReplicatedBlocks.TryGetValue(newBlock.BlockId, out block) )
                        {
                            dataServer.Blocks.Add(newBlock.BlockId);
                            block.DataServers.Add(dataServer);
                            if( block.DataServers.Count >= block.File.ReplicationFactor )
                            {
                                _log.InfoFormat("Block {0} is now fully replicated.", newBlock.BlockId);
                                _underReplicatedBlocks.Remove(newBlock.BlockId);
                            }
                        }
                        else
                        {
                            _log.WarnFormat("Block {0} is not pending and not underreplicated.", newBlock.BlockId);
                            response = new DeleteBlocksHeartbeatResponse(_fileSystem.FileSystemId, new Guid[] { newBlock.BlockId });
                        }
                    }
                }
            }

            return response;
        }

        private HeartbeatResponse ProcessBlockReport(DataServerInfo dataServer, BlockReportHeartbeatData blockReport)
        {
            /* The normal order of events is:
             * - Server checks in
             * - Server sends block report
             * - Server sends newblock heartbeat after it gets blocks
             * What can also happen (assume server already checked in)
             * - Server sends newblock
             * - Server restarts and sends block report before the block was committed.
             *   Because the commit count is different from the data server list, the block can get committed in the mean time.
             *   No action is necessary.
             * Also
             * - Server is receiving block
             * - Name server crashes and restarts
             * - Server sends newblock as the first heartbeat after the name server comes back up
             * - Server sends block report
             *   - Block could still be pending: this server is already registered: do nothing
             *   - Block no longer pending: do nothing.
             */
            if( dataServer.HasReportedBlocks )
                _log.Warn("Duplicate block report, ignoring.");
            else
            {
                List<Guid> invalidBlocks = null;
                _log.Info("Received block report.");
                dataServer.HasReportedBlocks = true;
                foreach( Guid block in blockReport.Blocks )
                {
                    BlockInfo info;
                    lock( _blocks )
                    {
                        if( _blocks.TryGetValue(block, out info) )
                        {
                            // See the explanation above for situations in which the DataServer is already present.
                            if( !info.DataServers.Contains(dataServer) )
                            {
                                _log.DebugFormat("Dataserver {0} has block ID {1}", dataServer.Address, block);
                                info.DataServers.Add(dataServer);
                                dataServer.Blocks.Add(block);
                                if( info.DataServers.Count >= info.File.ReplicationFactor )
                                {
                                    lock( _underReplicatedBlocks )
                                    {
                                        if( _underReplicatedBlocks.ContainsKey(block) )
                                        {
                                            _log.InfoFormat("Block {0} has reached sufficient replication level.", block);
                                            _underReplicatedBlocks.Remove(block);
                                        }
                                    }
                                }
                            }
                            else
                                _log.WarnFormat("Dataserver {0} re-reported block ID {1}", dataServer.Address, block);
                        }
                    }
                    if( info == null )
                    {
                        PendingBlock pendingBlock;
                        lock( _pendingBlocks )
                        {
                            if( _pendingBlocks.TryGetValue(block, out pendingBlock) )
                            {
                                Debug.Assert(pendingBlock.Block.DataServers.Contains(dataServer));
                                _log.WarnFormat("Dataserver {0} re-reported block ID {1}", dataServer.Address, block);
                            }
                        }
                        if( info == null )
                        {
                            _log.WarnFormat("Dataserver {0} reported unknown block {1}.", dataServer.Address, block);
                            if( invalidBlocks == null )
                                invalidBlocks = new List<Guid>();
                            invalidBlocks.Add(block);
                        }
                    }
                }
                CheckDisableSafeMode();
                if( invalidBlocks != null )
                    return new DeleteBlocksHeartbeatResponse(_fileSystem.FileSystemId, invalidBlocks);
            }
            return null;
        }

        private BlockAssignment AssignBlockToDataServers(BlockInfo block, bool useLocalReplica)
        {
            // If we are assigning a new block, the server context's client host name is the client that called AppendBlock and therefore the writer of the block.
            // If we are re-assigning an existing block, this is done internally so there won't be a server context.
            string writerHostName = null;
            if( ServerContext.Current != null )
                writerHostName = ServerContext.Current.ClientHostName;

            lock( _dataServers )
            {
                return _replicaPlacement.AssignBlockToDataServers(_dataServers.Values, block, writerHostName, useLocalReplica);
            }
        }

        private void ReassignBlock(BlockInfo block)
        {
            _log.InfoFormat("Reassigning new servers for underreplicated block {0}.", block.BlockId);
            if( block.DataServers.Count == 0 )
            {
                _log.WarnFormat("Cannot reassign block {0} because no data servers have this block.", block.BlockId);
            }
            else
            {
                BlockAssignment assignment = AssignBlockToDataServers(block, true); // Doesn't really matter what we pass for useLocalReplica, there's no writer.

                DataServerInfo source = block.DataServers[0];

                source.AddResponseForNextHeartbeat(new ReplicateBlockHeartbeatResponse(_fileSystem.FileSystemId, assignment));
            }
        }

        private static void ConfigureRemoting(DfsConfiguration config)
        {
            RpcHelper.RegisterServerChannels(config.FileSystem.Url.Port, config.NameServer.ListenIPv4AndIPv6);
            RpcHelper.RegisterService("NameServer", Instance);
            _log.Info("RPC server started.");
        }

        private void CheckSafeMode()
        {
            if( _safeMode )
                throw new SafeModeException("The name server is in safe mode.");
        }

        private void CheckDisableSafeMode()
        {
            int dataServerCount;
            lock( _dataServers )
                dataServerCount = (from server in _dataServers.Values where server.HasReportedBlocks select server).Count();
            int blockCount;
            lock( _underReplicatedBlocks )
                blockCount = _underReplicatedBlocks.Count;
            // TODO: After re-replication is implemented, we can disable safemode before having full replication.
            if( _safeMode && dataServerCount >= _replicationFactor && blockCount == 0 )
            {
                SafeMode = false;
            }
        }

        private static void MarkForDataServerDeletion(Guid block, BlockInfo info)
        {
            foreach( var server in info.DataServers )
            {
                server.AddBlockToDelete(block);
            }
        }

        private void RemoveDataServer(DataServerInfo info)
        {
            bool removed;
            lock( _dataServers )
            {
                removed = _dataServers.Remove(info.Address);
                NetworkTopology.RemoveNode(info);
                if( _dataServers.Count < _replicationFactor )
                    SafeMode = true;
            }
            // If removed is false, the server was already removed by another thread, so that thread is taking care of the blocks and we don't need to do that.
            if( removed )
            {
                lock( _blocks )
                {
                    lock( _underReplicatedBlocks )
                    {
                        lock( _pendingBlocks )
                        {
                            foreach( var blockID in info.Blocks )
                            {
                                bool pending = false;
                                BlockInfo blockInfo;
                                PendingBlock pendingBlock;
                                if( _pendingBlocks.TryGetValue(blockID, out pendingBlock) )
                                {
                                    blockInfo = pendingBlock.Block;
                                    pending = true;
                                }
                                else if( !_blocks.TryGetValue(blockID, out blockInfo) )
                                {
                                    _log.WarnFormat("Block {0} in the block list for server {1} isn't present in either the pending blocks or blocks collection.", blockID, info.Address);
                                    Debug.Assert(false); // This shouldn't happen, it means the block handling code has bugs.
                                }
                                else
                                {
                                    removed = blockInfo.DataServers.Remove(info);
                                    Debug.Assert(removed);
                                    if( !pending && blockInfo.DataServers.Count < blockInfo.File.ReplicationFactor && !_underReplicatedBlocks.ContainsKey(blockID) )
                                    {
                                        _log.InfoFormat("Block {0} is now under-replicated.", blockID);
                                        _underReplicatedBlocks.Add(blockID, blockInfo);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private void DataServerMonitorThread()
        {
            int timeout = Configuration.NameServer.DataServerTimeout * 1000;
            // This function will periodically check for dead data servers and underreplicated blocks.
            while( _running )
            {
                if( !SafeMode )
                {
                    CheckDeadDataServers();

                    CheckUnderReplicatedBlocks();
                }
                // The event can be used to force an immediate check, e.g. after forcibly removing a data server.
                _dataServerMonitorEvent.WaitOne(timeout, false);
            }
        }

        private void CheckUnderReplicatedBlocks()
        {
            lock( _blocks )
            {
                lock( _underReplicatedBlocks )
                {
                    if( _underReplicatedBlocks.Count > 0 )
                    {
                        foreach( var item in _underReplicatedBlocks.Values )
                        {
                            ReassignBlock(item);
                        }
                    }
                }
            }
        }

        private void CheckDeadDataServers()
        {
            lock( _dataServers )
            {
                List<DataServerInfo> deadServers = null;
                foreach( DataServerInfo server in _dataServers.Values )
                {
                    TimeSpan lastContact = DateTime.UtcNow - server.LastContactUtc;
                    if( lastContact.TotalSeconds > Configuration.NameServer.DataServerTimeout )
                    {
                        _log.WarnFormat("Data server {0} has not reported in {1} seconds and is considered dead.", server.Address, lastContact.TotalSeconds);
                        if( deadServers == null )
                            deadServers = new List<DataServerInfo>();
                        // We can't remove them here because that would break iteration.
                        deadServers.Add(server);
                    }
                }


                if( deadServers != null )
                {
                    foreach( DataServerInfo server in deadServers )
                    {
                        RemoveDataServer(server);
                    }
                }
            }
        }

        private void CreateCheckpointTimerCallback(object state)
        {
            CreateCheckpoint();
        }

        public void Dispose()
        {
            if (_fileSystem != null)
            {
                _fileSystem.Dispose();
            }

            if (_dataServerMonitorEvent != null)
            {
                _dataServerMonitorEvent.Dispose();
            }

            if (_checkpointTimer != null)
            {
                _checkpointTimer.Dispose();
            }
        }
    }
}
