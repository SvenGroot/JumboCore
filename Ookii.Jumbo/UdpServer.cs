// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Globalization;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Abstract base class for a server that handles incoming UDP messages.
    /// </summary>
    public abstract class UdpServer : IDisposable
    {
        #region Nested types

        private sealed class SlimUdpClient : IDisposable
        {
            private readonly Socket _socket;
            private readonly byte[] _buffer = new byte[0x10000];

            public SlimUdpClient(IPAddress localAddress, int port, bool allowAddressReuse)
            {
                _socket = new Socket(localAddress.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, allowAddressReuse);
                _socket.Bind(new IPEndPoint(localAddress, port));
            }

            public IAsyncResult BeginReceive(AsyncCallback callback, object state)
            {
                EndPoint remote;
                if( _socket.AddressFamily == AddressFamily.InterNetworkV6 )
                    remote = new IPEndPoint(IPAddress.IPv6Any, 0);
                else
                    remote = new IPEndPoint(IPAddress.Any, 0);

                return _socket.BeginReceiveFrom(_buffer, 0, 0x10000, SocketFlags.None, ref remote, callback, state);
            }

            public byte[] EndReceive(IAsyncResult ar, out IPEndPoint remoteEndPoint)
            {
                EndPoint remote;
                if( _socket.AddressFamily == AddressFamily.InterNetworkV6 )
                    remote = new IPEndPoint(IPAddress.IPv6Any, 0);
                else
                    remote = new IPEndPoint(IPAddress.Any, 0);

                int count = _socket.EndReceiveFrom(ar, ref remote);
                remoteEndPoint = (IPEndPoint)remote;
                byte[] result = new byte[count];
                Buffer.BlockCopy(_buffer, 0, result, 0, count);
                return result;
            }

            public void Dispose()
            {
                ((IDisposable)_socket).Dispose();
                GC.SuppressFinalize(this);
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(UdpServer));

        private readonly SlimUdpClient[] _sockets;
        private readonly AsyncCallback _callback;

        /// <summary>
        /// Initializes a new instance of the <see cref="UdpServer"/> class.
        /// </summary>
        /// <param name="localAddresses">The local addresses to bind to.</param>
        /// <param name="port">The port to bind to.</param>
        /// <param name="allowAddressReuse">If set to <see langword="true"/>, allows the sockets to be bound to an address that is already in use.</param>
        protected UdpServer(IPAddress[] localAddresses, int port, bool allowAddressReuse)
        {
            if( localAddresses == null )
                throw new ArgumentNullException("localAddresses");
            if( localAddresses.Length == 0 )
                throw new ArgumentException("You must specify at least one address to bind to.", "localAddresses");

            _callback = new AsyncCallback(ReceiveFromCallback);
            _sockets = new SlimUdpClient[localAddresses.Length];
            int x = 0;
            foreach( IPAddress localAddress in localAddresses )
            {
                _sockets[x] = new SlimUdpClient(localAddress, port, allowAddressReuse);
            }
        }

        /// <summary>
        /// Starts listening for UDP datagrams.
        /// </summary>
        public void Start()
        {
            foreach( SlimUdpClient socket in _sockets )
            {
                socket.BeginReceive(_callback, socket);
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// When implemented in a derived class, handles processing of a UDP message.
        /// </summary>
        /// <param name="message">The message data.</param>
        /// <param name="remoteEndPoint">The remote end point from where the message was received.</param>
        protected abstract void HandleMessage(byte[] message, IPEndPoint remoteEndPoint);

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if( disposing )
            {
                foreach( SlimUdpClient socket in _sockets )
                {
                    socket.Dispose();
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void ReceiveFromCallback(IAsyncResult ar)
        {
            try
            {
                SlimUdpClient client = (SlimUdpClient)ar.AsyncState;
                IPEndPoint remoteEndPoint;
                byte[] message;
                message = client.EndReceive(ar, out remoteEndPoint);
                client.BeginReceive(_callback, client);
                try
                {
                    HandleMessage(message, remoteEndPoint);
                }
                catch( Exception ex )
                {
                    _log.Error(string.Format(CultureInfo.InvariantCulture, "Error handling UDP message from {0}.", remoteEndPoint), ex);
                }
            }
            catch( ObjectDisposedException )
            {
                // Thrown if the BeginReceiveFrom call was cancelled by the Dispose method.
            }
        }
    }
}
