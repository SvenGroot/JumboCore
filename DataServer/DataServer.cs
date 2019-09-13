﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs;
using System.Threading;
using System.Configuration;
using System.IO;
using Ookii.Jumbo;
using System.Net.Sockets;
using System.Net;
using Ookii.Jumbo.Rpc;
using Ookii.Jumbo.Dfs.FileSystem;

namespace DataServerApplication
{
    public class DataServer
    {
        private const int _heartbeatInterval = 3000;
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(DataServer));
        private readonly string _blockStorageDirectory;
        private readonly string _temporaryBlockStorageDirectory;
        private readonly int _port;
        private readonly DfsConfiguration _config;
        private Guid _fileSystemId;

        private INameServerHeartbeatProtocol _nameServer;
        private List<HeartbeatData> _pendingHeartbeatData = new List<HeartbeatData>();

        private List<Guid> _blocks = new List<Guid>();
        private List<Guid> _pendingBlocks = new List<Guid>();
        private BlockServer _blockServer; // listens for TCP connections.
        private volatile bool _running;
        private readonly Queue<ReplicateBlockHeartbeatResponse> _blocksToReplicate = new Queue<ReplicateBlockHeartbeatResponse>();
        private readonly Thread _replicateBlocksThread;

        public DataServer()
            : this(DfsConfiguration.GetConfiguration())
        {
        }

        public DataServer(DfsConfiguration config)
        {
            if( config == null )
                throw new ArgumentNullException("config");

            _config = config;
            _blockStorageDirectory = config.DataServer.BlockStorageDirectory;
            if( string.IsNullOrWhiteSpace(_blockStorageDirectory) )
                throw new InvalidOperationException("DataServer block storage path is not configured.");
            _temporaryBlockStorageDirectory = Path.Combine(_blockStorageDirectory, "temp");
            System.IO.Directory.CreateDirectory(_temporaryBlockStorageDirectory);
            _port = config.DataServer.Port;
            _nameServer = DfsClient.CreateNameServerHeartbeatClient(config);

            _replicateBlocksThread = new Thread(ReplicateBlocksThread) { Name = "ReplicateBlockThread", IsBackground = true };

            LoadBlocks();
        }

        public ServerAddress LocalAddress { get; private set; }

        public void Run()
        {
            _running = true;
            LocalAddress = new ServerAddress(System.Net.Dns.GetHostName(), _port);

            _log.Info("Data server main loop starting.");

            if( _running )
            {
                IPAddress[] addresses = TcpServer.GetDefaultListenerAddresses(_config.DataServer.ListenIPv4AndIPv6);
                _blockServer = new BlockServer(this, addresses, _config.DataServer.Port);
                _blockServer.Start();

                _replicateBlocksThread.Start();

                AddDataForNextHeartbeat(new InitialHeartbeatData(_fileSystemId));

                while( _running )
                {
                    int start = Environment.TickCount;
                    SendHeartbeat();
                    int end = Environment.TickCount;
                    if( end - start > 500 )
                        _log.WarnFormat("Long heartbeat time: {0}", end - start);
                    Thread.Sleep(_heartbeatInterval);
                }
            }
        }

        public void Abort()
        {
            if( _blockServer != null )
            {
                _blockServer.Stop();
                _blockServer = null;
            }
            _running = false;
            RpcHelper.AbortRetries();
            RpcHelper.CloseConnections();
            lock( _blocksToReplicate )
            {
                Monitor.Pulse(_blocksToReplicate);
            }
            try
            {
                _replicateBlocksThread.Join();
            }
            catch( ThreadStateException )
            {
            }
        }

        public FileStream AddNewBlock(Guid blockID)
        {
            lock( _blocks )
            lock( _pendingBlocks )
            {
                if( _blocks.Contains(blockID) || _pendingBlocks.Contains(blockID) )
                    throw new ArgumentException("Existing block ID.");
                _pendingBlocks.Add(blockID);
                System.IO.Directory.CreateDirectory(_temporaryBlockStorageDirectory);
                return System.IO.File.Create(Path.Combine(_temporaryBlockStorageDirectory, blockID.ToString()), (int)_config.DataServer.WriteBufferSize.Value);
            }
        }

        public FileStream OpenBlock(Guid blockID)
        {
            return new FileStream(GetBlockFileName(blockID), FileMode.Open, FileAccess.Read, FileShare.Read, (int)_config.DataServer.ReadBufferSize.Value);
        }

        public int GetBlockSize(Guid blockID)
        {
            lock( _blocks )
            {
                if( !_blocks.Contains(blockID) )
                    throw new ArgumentException("Invalid block.");
            }

            return (int)new FileInfo(GetBlockFileName(blockID)).Length;
        }

        public void CompleteBlock(Guid blockID, int size)
        {
            lock( _blocks )
            lock( _pendingBlocks )
            {
                if( !_pendingBlocks.Contains(blockID) || _blocks.Contains(blockID) )
                    throw new ArgumentException("Invalid block ID.");

                _pendingBlocks.Remove(blockID);
                System.IO.File.Move(Path.Combine(_temporaryBlockStorageDirectory, blockID.ToString()), GetBlockFileName(blockID));
                _blocks.Add(blockID);
            }
            NewBlockHeartbeatData data = new NewBlockHeartbeatData() { BlockId = blockID, Size = size };
            GetDiskUsage(data);
            AddDataForNextHeartbeat(data);
            // We send the heartbeat immediately so the client knows that when the server comes back to him, the name server
            // knows about the block being committed.
            SendHeartbeat();
        }

        public void RemoveBlockIfPending(Guid blockID)
        {
            lock( _pendingBlocks )
            {
                if( _pendingBlocks.Contains(blockID) )
                {
                    _pendingBlocks.Remove(blockID);
                    string blockFile = Path.Combine(_temporaryBlockStorageDirectory, blockID.ToString());
                    if( System.IO.File.Exists(blockFile) )
                        System.IO.File.Delete(blockFile);
                }
            }
        }

        private void SendHeartbeat()
        {
            //_log.Debug("Sending heartbeat to name server.");
            HeartbeatData[] data = null;
            lock( _pendingHeartbeatData )
            {
                if( _pendingHeartbeatData.Count > 0 )
                {
                    data = _pendingHeartbeatData.ToArray();
                    _pendingHeartbeatData.Clear();
                }
            }
            HeartbeatResponse[] response = null;
            RpcHelper.TryRemotingCall(() => response = _nameServer.Heartbeat(LocalAddress, data), _heartbeatInterval, -1);
            if( response != null )
                ProcessResponses(response);
        }

        private void ProcessResponses(HeartbeatResponse[] responses)
        {
            foreach( var response in responses )
                ProcessResponse(response);
        }

        private void ProcessResponse(HeartbeatResponse response)
        {
            CheckFileSystemId(response);

            switch( response.Command )
            {
            case DataServerHeartbeatCommand.ReportBlocks:
                _log.Info("Received ReportBlocks command.");
                BlockReportHeartbeatData data;
                lock( _blocks )
                {
                    data = new BlockReportHeartbeatData(_blocks);
                    GetDiskUsage(data);
                }
                AddDataForNextHeartbeat(data);
                break;
            case DataServerHeartbeatCommand.DeleteBlocks:
                _log.Info("Received DeleteBlocks command.");
                ThreadPool.QueueUserWorkItem(state => DeleteBlocks((IEnumerable<Guid>)state), ((DeleteBlocksHeartbeatResponse)response).Blocks);
                break;
            case DataServerHeartbeatCommand.ReplicateBlock:
                _log.Info("Received ReplicateBlock command.");
                lock( _blocksToReplicate )
                {
                    _blocksToReplicate.Enqueue((ReplicateBlockHeartbeatResponse)response);
                    Monitor.Pulse(_blocksToReplicate);
                }
                break;
            }
        }

        private void CheckFileSystemId(HeartbeatResponse response)
        {
            if( _fileSystemId == Guid.Empty )
            {
                _fileSystemId = response.FileSystemId;
                File.WriteAllBytes(Path.Combine(_config.DataServer.BlockStorageDirectory, "fsid"), _fileSystemId.ToByteArray());
                _log.InfoFormat("File system ID set to {0:B}.", _fileSystemId);
            }
            else if( _fileSystemId != response.FileSystemId )
            {
                throw new InvalidOperationException(string.Format("NameServer reported file system ID {0:B}; expecting {1:B}.", response.FileSystemId, _fileSystemId));
            }
        }

        private void AddDataForNextHeartbeat(HeartbeatData data)
        {
            lock( _pendingHeartbeatData )
            {
                _pendingHeartbeatData.Add(data);
            }
        }

        private void LoadBlocks()
        {
            // Since this'll be likely only done on object construction, the lock isn't strictly needed.
            // It doesn't hurt though.
            lock( _blocks )
            {
                string[] files = System.IO.Directory.GetFiles(_blockStorageDirectory);
                string fsIdFile = Path.Combine(_blockStorageDirectory, "fsid");
                if( File.Exists(fsIdFile) )
                {
                    _fileSystemId = new Guid(File.ReadAllBytes(fsIdFile));
                    _log.InfoFormat("File system ID: {0:B}.", _fileSystemId);
                    _log.InfoFormat("Loading block list...");

                    foreach( string file in files )
                    {
                        string fileName = Path.GetFileName(file);
                        if( fileName != "fsid" )
                        {
                            try
                            {
                                Guid blockID = new Guid(fileName);
                                _log.DebugFormat("- Block ID: {0}", blockID);
                                _blocks.Add(blockID);
                            }
                            catch( FormatException )
                            {
                                _log.WarnFormat("The name of file '{0}' in the block storage directory is not a valid GUID.", fileName);
                            }
                        }
                    }
                }
                else
                {
                    if( files.Length > 0 )
                        throw new InvalidOperationException("DataServer is not part of a file system but block directory is not empty.");
                    _log.InfoFormat("DataServer is not yet part of a file system.");
                    _fileSystemId = Guid.Empty;
                }
            }
        }

        private void DeleteBlocks(IEnumerable<Guid> blocks)
        {
            lock( _blocks )
            {
                foreach( var block in blocks )
                {
                    _log.InfoFormat("Removing block {0}.", block);
                    _blocks.Remove(block);
                }
            }
            foreach( var block in blocks )
            {
                try
                {
                    System.IO.File.Delete(GetBlockFileName(block));
                }
                catch( IOException ex )
                {
                    _log.Error(string.Format("Failed to delete block {0}.", block), ex);
                }
            }
            StatusHeartbeatData statusData = new StatusHeartbeatData();
            GetDiskUsage(statusData);
            AddDataForNextHeartbeat(statusData);
        }

        private void GetDiskUsage(StatusHeartbeatData data)
        {
            DriveSpaceInfo info = new DriveSpaceInfo(_blockStorageDirectory);
            data.DiskSpaceFree = info.AvailableFreeSpace;
            data.DiskSpaceTotal = info.TotalSize;
            lock( _blocks )
            {
                foreach( var blockID in _blocks )
                {
                    string blockFile = GetBlockFileName(blockID);
                    data.DiskSpaceUsed += new FileInfo(blockFile).Length;
                }
            }
        }

        private string GetBlockFileName(Guid blockID)
        {
            return Path.Combine(_blockStorageDirectory, blockID.ToString());
        }

        private void ReplicateBlocksThread()
        {
            while( _running )
            {
                try
                {
                    ReplicateBlockHeartbeatResponse response;
                    lock( _blocksToReplicate )
                    {
                        while( _running && _blocksToReplicate.Count == 0 )
                            Monitor.Wait(_blocksToReplicate);
                        if( !_running )
                            return;
                        response = _blocksToReplicate.Dequeue();
                    }

                    lock( _blocks )
                    {
                        if( !_blocks.Contains(response.BlockAssignment.BlockId) )
                        {
                            _log.WarnFormat("Received a command to replicate an unknown block with ID {0}.", response.BlockAssignment.BlockId);
                            return;
                        }
                    }
                    _log.InfoFormat("Replicating block {0} to {1} data servers; first is {2}.", response.BlockAssignment.BlockId, response.BlockAssignment.DataServers.Count, response.BlockAssignment.DataServers[0]);
                    Packet packet = new Packet();
                    using( BlockSender sender = new BlockSender(response.BlockAssignment) )
                    using( FileStream file = System.IO.File.OpenRead(GetBlockFileName(response.BlockAssignment.BlockId)) )
                    using( BinaryReader reader = new BinaryReader(file) )
                    {
                        do
                        {
                            packet.Read(reader, PacketFormatOption.ChecksumOnly, true);
                            packet.SequenceNumber++;
                            sender.SendPacket(packet);
                        } while( !packet.IsLastPacket );
                        sender.WaitForAcknowledgements();
                    }
                    _log.InfoFormat("Finished replicating block {0}.", response.BlockAssignment.BlockId);
                }
                catch( Exception ex )
                {
                    _log.Error("Failed to replicate block.", ex);
                }
            }
        }

	  /*private void StatusUpdateThread()
        {
            int interval = _config.DataServer.StatusUpdateInterval * 1000;
            while( _running )
            {
                if( _abortEvent.WaitOne(interval, false) )
                    return;

                StatusHeartbeatData data = new StatusHeartbeatData();
                GetDiskUsage(data);
                _log.InfoFormat("Sending updated disk space status to the name server: total = {0}, free = {1}, DFS used = {2}.", data.DiskSpaceTotal, data.DiskSpaceFree, data.DiskSpaceUsed);
                AddDataForNextHeartbeat(data);
            }
			}*/
    }
}