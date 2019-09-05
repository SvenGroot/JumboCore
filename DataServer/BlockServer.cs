﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using Ookii.Jumbo.Dfs;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Diagnostics;
using Ookii.Jumbo;

namespace DataServerApplication
{
    /// <summary>
    /// Provides a TCP server that clients can use to read and write blocks to the data server.
    /// </summary>
    class BlockServer : TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BlockServer));

        private DataServer _dataServer;

        public BlockServer(DataServer dataServer, IPAddress[] localAddresses, int port)
            : base(localAddresses, port)
        {
            if( dataServer == null )
                throw new ArgumentNullException("dataServer");
            _log.InfoFormat("Starting block server on {0}", localAddresses);

            _dataServer = dataServer;
        }

        protected override void HandleConnection(TcpClient client)
        {
            try
            {
                using( NetworkStream stream = client.GetStream() )
                {
                    // TODO: Return error codes on invalid header etc.
                    BinaryFormatter formatter = new BinaryFormatter();
                    DataServerClientProtocolHeader header = (DataServerClientProtocolHeader)formatter.Deserialize(stream);

                    switch( header.Command )
                    {
                    case DataServerCommand.WriteBlock:
                        client.LingerState = new LingerOption(true, 10);
                        client.NoDelay = true;
                        DataServerClientProtocolWriteHeader writeHeader = header as DataServerClientProtocolWriteHeader;
                        if( writeHeader != null )
                        {
                            ReceiveBlock(stream, writeHeader);
                        }
                        break;
                    case DataServerCommand.ReadBlock:
                        DataServerClientProtocolReadHeader readHeader = header as DataServerClientProtocolReadHeader;
                        if( readHeader != null )
                        {
                            SendBlock(stream, readHeader);
                        }
                        break;
                    case DataServerCommand.GetLogFileContents:
                        DataServerClientProtocolGetLogFileContentsHeader logHeader = header as DataServerClientProtocolGetLogFileContentsHeader;
                        if( logHeader != null )
                            SendLogFile(stream, logHeader);
                        break;
                    }
                }
            }
            catch( Exception ex )
            {
                _log.Error("An error occurred handling a client connection.", ex);
            }
        }

        private void ReceiveBlock(NetworkStream stream, DataServerClientProtocolWriteHeader header)
        {
            _log.InfoFormat("Block write command received for block {0}", header.BlockId);
            int blockSize = 0;
            //DataServerClientProtocolResult forwardResult;

            bool acceptedHeader = false;
            using( BinaryReader clientReader = new BinaryReader(stream) )
            using( BinaryWriter clientWriter = new BinaryWriter(stream) )
            {
                try
                {
                    if( header.DataServers.Count == 0 || !header.DataServers[0].Equals(_dataServer.LocalAddress) )
                    {
                        _log.ErrorFormat("This server was not the first server in the list of remaining servers for the block.");
                        clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                        return;
                    }
                    using( FileStream blockFile = _dataServer.AddNewBlock(header.BlockId) )
                    using( BinaryWriter fileWriter = new BinaryWriter(blockFile) )
                    using( BlockSender forwarder = new BlockSender(header.BlockId, header.DataServers.Skip(1), clientWriter) )
                    {
                        // Accept the header
                        clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                        acceptedHeader = true;

                        if( !ReceivePackets(header, ref blockSize, clientWriter, clientReader, forwarder, fileWriter) )
                            return;

                        forwarder.WaitForAcknowledgements();
                    }

                    _dataServer.CompleteBlock(header.BlockId, blockSize);
                    clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    _log.InfoFormat("Writing block {0} complete.", header.BlockId);
                }
                catch( Exception ex )
                {
                    _log.Error("Error occurred receiving block.", ex);
                    try
                    {
                        SendErrorResultAndWaitForConnectionClosed(clientWriter, clientReader, acceptedHeader);
                    }
                    catch( Exception )
                    {
                    }
                    throw;
                }
                finally
                {
                    _dataServer.RemoveBlockIfPending(header.BlockId);
                }
            }

        }

        private static void SendErrorResultAndWaitForConnectionClosed(BinaryWriter clientWriter, BinaryReader reader, bool useSequenceError)
        {
            if( useSequenceError )
                clientWriter.Write(-1L);
            else
                clientWriter.WriteResult(DataServerClientProtocolResult.Error);
            Thread.Sleep(5000); // Wait some time so the client can catch the error before the socket is closed; this is only necessary
                                // on Win32 it seems, but it doesn't harm anything.
        }

        private static bool ReceivePackets(DataServerClientProtocolWriteHeader header, ref int blockSize, BinaryWriter clientWriter, BinaryReader clientReader, BlockSender forwarder, BinaryWriter fileWriter)
        {
            Packet packet = new Packet();
            do
            {
                try
                {
                    packet.Read(clientReader, PacketFormatOption.Default, forwarder.IsResponseOnly); // Only the last server in the chain needs to verify checksums
                }
                catch( InvalidPacketException ex )
                {
                    _log.Error(ex.Message);
                    forwarder.Cancel();
                    SendErrorResultAndWaitForConnectionClosed(clientWriter, clientReader, true);
                    return false;
                }

                blockSize += packet.Size;

                if( forwarder != null )
                {
                    if( !CheckForwarderError(header, forwarder) )
                    {
                        SendErrorResultAndWaitForConnectionClosed(clientWriter, clientReader, true);
                        return false;
                    }
                    forwarder.SendPacket(packet);
                }

                packet.Write(fileWriter, PacketFormatOption.ChecksumOnly);
            } while( !packet.IsLastPacket );
            return true;
        }

        private static bool CheckForwarderError(DataServerClientProtocolWriteHeader header, BlockSender forwarder)
        {
            if( forwarder.ServerStatus == DataServerClientProtocolResult.Error )
            {
                //if( forwarder.LastException != null )
                //    _log.Error(string.Format("An error occurred forwarding block to server {0}.", header.DataServers[1]), forwarder.LastException);
                //else
                _log.ErrorFormat("The next data server {0} encountered an error writing a packet of block {1}.", header.DataServers[1], header.BlockId);
                return false;
            }
            return true;
        }

        private void SendBlock(NetworkStream clientStream, DataServerClientProtocolReadHeader header)
        {
            _log.InfoFormat("Block read command received: block {0}, offset {1}, size {2}.", header.BlockId, header.Offset, header.Size);
            int packetOffset = header.Offset / Packet.PacketSize;
            int offset = packetOffset * Packet.PacketSize; // Round down to the nearest packet.
            // File offset has to take CRCs into account.
            int fileOffset = packetOffset * (Packet.PacketSize + sizeof(uint));

            int endPacketOffset = 0;
            int endOffset = 0;
            int endFileOffset = 0;

            try
            {
                using( FileStream blockFile = _dataServer.OpenBlock(header.BlockId) )
                using( BinaryReader blockReader = new BinaryReader(blockFile) )
                using( Ookii.Jumbo.IO.WriteBufferedStream bufferedStream = new Ookii.Jumbo.IO.WriteBufferedStream(clientStream) )
                using( BinaryWriter clientWriter = new BinaryWriter(bufferedStream) )
                {
                    // Check if the requested offset is in range. To do this, we take the computed offset of the 
                    // first packet to send (fileOffset) and add the offset into that first packet (header.Offset - offset) to it.
                    if( fileOffset + header.Offset - offset > blockFile.Length )
                    {
                        _log.ErrorFormat("Client requested offset {0} (after correction) larger than block file length {1}.", fileOffset + header.Offset - offset, blockFile.Length);
                        clientWriter.WriteResult(DataServerClientProtocolResult.OutOfRange);
                        return;
                    }

                    //_log.DebugFormat("Block file opened, beginning send.");
                    if( header.Size >= 0 )
                    {
                        endPacketOffset = (header.Offset + header.Size) / Packet.PacketSize;
                    }
                    else
                    {
                        endPacketOffset = (int)(blockFile.Length / (Packet.PacketSize + sizeof(uint)));
                    }
                    endOffset = endPacketOffset * Packet.PacketSize;
                    endFileOffset = endPacketOffset * (Packet.PacketSize + sizeof(uint));

					//_log.DebugFormat("Block file length: {0}, offset: {1}, end offset = {2}", blockFile.Length, fileOffset, endFileOffset);

                    if( fileOffset > blockFile.Length || endFileOffset > blockFile.Length )
                    {
                        _log.Error("Requested offsets are out of range.");
                        clientWriter.WriteResult(DataServerClientProtocolResult.OutOfRange);
                        return;
                    }

                    blockFile.Seek(fileOffset, SeekOrigin.Begin);
                    int sizeRemaining = endOffset - offset;
                    Packet packet = new Packet();
                    clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                    clientWriter.Write(offset);
                    try
                    {
                        do
                        {
                            packet.Read(blockReader, PacketFormatOption.ChecksumOnly, false);

                            if( sizeRemaining == 0 )
                                packet.IsLastPacket = true;

                            clientWriter.WriteResult(DataServerClientProtocolResult.Ok);
                            packet.Write(clientWriter, PacketFormatOption.NoSequenceNumber);

                            // assertion to check if we don't jump over zero.
                            System.Diagnostics.Debug.Assert(sizeRemaining > 0 ? sizeRemaining - packet.Size >= 0 : true);
                            sizeRemaining -= packet.Size;
                        } while( !packet.IsLastPacket );
                    }
                    catch( InvalidPacketException )
                    {
                        clientWriter.WriteResult(DataServerClientProtocolResult.Error);
                        return;
                    }
                }
            }
            catch( IOException ex )
            {
                if( ex.InnerException is SocketException )
                {
                    SocketException socketEx = (SocketException)ex.InnerException;
                    if( socketEx.ErrorCode == (int)SocketError.ConnectionAborted || socketEx.ErrorCode == (int)SocketError.ConnectionReset )
                    {
                        _log.Info("The connection was closed by the remote host.");
                        return;
                    }
                }
                throw;

            }

            _log.InfoFormat("Finished sending block {0}", header.BlockId);
        }

        private void SendLogFile(NetworkStream stream, DataServerClientProtocolGetLogFileContentsHeader header)
        {
            using( Stream logStream = LogFileHelper.GetLogFileStream("DataServer", header.Kind, header.MaxSize) )
            {
                if( logStream != null )
                    logStream.CopyTo(stream);
            }
        }
    }
}
