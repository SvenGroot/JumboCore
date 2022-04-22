using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Handles sending a block to a data server, and sending acknowledgements to a client.
    /// </summary>
    public sealed class BlockSender : IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(BlockSender));

        private readonly Guid _blockId;
        private readonly ServerAddress[] _dataServers;
        private readonly BinaryWriter _clientWriter; // Not owned by this class; don't dispose.
        private readonly TcpClient _serverClient;
        private readonly NetworkStream _serverStream;
        private readonly BinaryWriter _serverWriter;
        private readonly BinaryReader _serverReader;

        private readonly BlockingCollection<long> _pendingAcknowledgements = new BlockingCollection<long>();
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private readonly Thread _acknowledgementThread;

        private bool _disposed;
        private DataServerClientProtocolResult _serverStatus;
        private bool _hasLastPacket;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockSender"/> class.
        /// </summary>
        /// <param name="assignment">The block assignment.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public BlockSender(BlockAssignment assignment)
            : this(assignment.BlockId, assignment.DataServers, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockSender"/> class.
        /// </summary>
        /// <param name="blockId">The <see cref="Guid"/> of the block to send.</param>
        /// <param name="dataServers">The data servers that the block should be forwarded to. May be an empty list.</param>
        /// <param name="clientWriter">The writer to use to forward acknowledgements. May be null.</param>
        public BlockSender(Guid blockId, IEnumerable<ServerAddress> dataServers, BinaryWriter clientWriter)
        {
            _blockId = blockId;
            _clientWriter = clientWriter;
            _dataServers = dataServers == null ? Array.Empty<ServerAddress>() : dataServers.ToArray();
            if (_dataServers.Length > 0)
            {
                var server = _dataServers[0];
                try
                {
                    _serverClient = new TcpClient(server.HostName, server.Port);
                    _serverStream = _serverClient.GetStream();
                    _serverReader = new BinaryReader(_serverStream);
                    _serverWriter = new BinaryWriter(_serverStream);
                    if (!WriteHeader())
                        throw new DfsException(string.Format(CultureInfo.CurrentCulture, "There was an error connecting to the downstream data server {0}.", server));
                }
                catch (Exception ex)
                {
                    throw new DfsException(string.Format(CultureInfo.CurrentCulture, "There was an error connecting to the downstream data server {0}.", server), ex);
                }
            }
            _acknowledgementThread = new Thread(AcknowledgementThread) { IsBackground = true, Name = "BlockSender_AcknowledgementTread" };
            _acknowledgementThread.Start();
        }

        /// <summary>
        /// Gets a value indicating whether this instance only sends responses, and doesn't send the data to any server.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if this instance only sends responses; otherwise, <see langword="false" />.
        /// </value>
        public bool IsResponseOnly
        {
            get { return _serverClient == null; }
        }

        /// <summary>
        /// Gets the server status.
        /// </summary>
        /// <value>
        /// One of the <see cref="DataServerClientProtocolResult"/> values that indicates the server status.
        /// </value>
        public DataServerClientProtocolResult ServerStatus
        {
            get { return _serverStatus; }
        }

        /// <summary>
        /// Sends the packet to the server, and queues the acknowledgement.
        /// </summary>
        /// <param name="packet">The packet.</param>
        public void SendPacket(Packet packet)
        {
            if (packet == null)
                throw new ArgumentNullException(nameof(packet));
            ThrowIfErrorOccurred();

            if (_hasLastPacket)
                throw new InvalidOperationException("The last packet has been sent.");

            if (_serverWriter != null)
                packet.Write(_serverWriter, PacketFormatOption.Default);

            _pendingAcknowledgements.Add(packet.SequenceNumber, _cancellation.Token);
            if (packet.IsLastPacket)
            {
                _hasLastPacket = true;
                _pendingAcknowledgements.CompleteAdding();
            }
        }

        /// <summary>
        /// Waits for acknowledgements.
        /// </summary>
        public void WaitForAcknowledgements()
        {
            ThrowIfErrorOccurred();
            _acknowledgementThread.Join();
            ThrowIfErrorOccurred();
        }

        /// <summary>
        /// Cancels this instance.
        /// </summary>
        public void Cancel()
        {
            _cancellation.Cancel();
        }

        /// <summary>
        /// Throws an exception if an error occurred.
        /// </summary>
        public void ThrowIfErrorOccurred()
        {
            if (_serverStatus != DataServerClientProtocolResult.Ok)
                throw new DfsException("There was an error sending the block to the downstream data server.");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
                if (disposing)
                {
                    _cancellation.Cancel();
                    if (_serverWriter != null)
                        _serverWriter.Dispose();
                    if (_serverReader != null)
                        _serverReader.Dispose();
                    if (_serverStream != null)
                        _serverStream.Dispose();
                    if (_serverClient != null)
                        ((IDisposable)_serverClient).Dispose();

                    _acknowledgementThread.Join();
                    _cancellation.Dispose();
                    _pendingAcknowledgements.Dispose();
                }
            }
        }

        private bool WriteHeader()
        {
            // Send the header
            var header = new DataServerClientProtocolWriteHeader(_dataServers);
            header.BlockId = _blockId;
            var formatter = new BinaryFormatter();
            formatter.Serialize(_serverStream, header);
            _serverStream.Flush();

            return ReadResult();
        }

        private bool ReadResult()
        {
            var result = (DataServerClientProtocolResult)_serverReader.ReadInt16();
            if (result != DataServerClientProtocolResult.Ok)
            {
                _serverStatus = result;
                return false;
            }
            return true;
        }

        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(this.GetType().FullName);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void AcknowledgementThread()
        {
            try
            {
                if (_serverClient != null)
                {
                    while (!_cancellation.IsCancellationRequested && _pendingAcknowledgements.TryTake(out var expected, Timeout.Infinite, _cancellation.Token))
                    {
                        var sequenceNumber = _serverReader.ReadInt64();
                        if (sequenceNumber != expected)
                        {
                            _log.ErrorFormat("Block sender received unexpected sequence number acknowledgement {0}", sequenceNumber);
                            _serverStatus = DataServerClientProtocolResult.Error;
                        }

                        if (_clientWriter != null)
                        {
                            _clientWriter.Write(sequenceNumber);
                        }
                    }
                    if (_cancellation.IsCancellationRequested)
                        _log.Warn("The block sender was cancelled.");
                    else
                    {
                        Debug.Assert(_pendingAcknowledgements.IsCompleted);

                        // Read the final Ok.
                        ReadResult();
                    }
                }
                else
                {
                    while (!_cancellation.IsCancellationRequested && _pendingAcknowledgements.TryTake(out var sequenceNumber, Timeout.Infinite, _cancellation.Token))
                    {
                        _clientWriter.Write(sequenceNumber);
                    }
                    Debug.Assert(_pendingAcknowledgements.IsCompleted || _cancellation.IsCancellationRequested);

                    // Final Ok is written by BlockServer, not us.
                }
            }
            catch (OperationCanceledException)
            {
                _log.Warn("The block sender was cancelled.");
            }
            catch (Exception ex)
            {
                _log.Error("Error while waiting for or writing acknowledgements.", ex);
                _serverStatus = DataServerClientProtocolResult.Error;
            }
        }
    }
}
