// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Runtime.CompilerServices;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Abstract base class for a server that accepts incoming TCP connections.
    /// </summary>
    public abstract class TcpServer
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TcpServer));

        private TcpListener[] _listeners;
        private volatile bool _running;
        private Thread[] _listenerThreads;
        private readonly int _maxConnections;
        private int _connections;
        private static readonly byte[] _connectionAccepted = new byte[] { 0, 0, 0, 1 };
        private static readonly byte[] _connectionRejected = new byte[] { 0, 0, 0, 0 };

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpServer"/> class with the specified local address, port, and maximum number of connections.
        /// </summary>
        /// <param name="localAddresses">The local IP address that the server should bind to.</param>
        /// <param name="port">The port to listen on.</param>
        /// <param name="maxConnections">The maximum number of simultaneous connections that the server will allow, or zero to have no maximum.</param>
        /// <remarks>
        /// <para>
        ///   If <paramref name="maxConnections"/> is larger than zero, the <see cref="TcpServer"/> class will maintain a count of
        ///   connected clients on all specified end points. If a client tries to connect when the count is equal to <paramref name="maxConnections"/>,
        ///   the connection will be refused.
        /// </para>
        /// <para>
        ///   If <paramref name="maxConnections"/> is larger than zero, the <see cref="TcpServer"/> will send a 4-byte value (to keep the remaining data
        ///   word-aligned) that is zero to indicate the connection was rejected, or non-zero to indicate it was accepted. When <paramref name="maxConnections"/>
        ///   is zero, this value is not sent.
        /// </para>
        /// </remarks>
        protected TcpServer(IPAddress[] localAddresses, int port, int maxConnections)
        {
            if( localAddresses == null )
                throw new ArgumentNullException("localAddresses");
            if( localAddresses.Length == 0 )
                throw new ArgumentException("You must specify at least one address to listen on.", "localAddresses");
            if( maxConnections < 0 )
                throw new ArgumentOutOfRangeException("maxConnections", "The maximum number of connections must be zero or more.");

            _listeners = new TcpListener[localAddresses.Length];
            for( int x = 0; x < localAddresses.Length; ++x )
                _listeners[x] = new TcpListener(localAddresses[x], port);
            _maxConnections = maxConnections;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TcpServer"/> class with the specified local address and port.
        /// </summary>
        /// <param name="localAddresses">The local IP address that the server should bind to.</param>
        /// <param name="port">The port to listen on.</param>
        protected TcpServer(IPAddress[] localAddresses, int port)
            : this(localAddresses, port, 0)
        {
        }

        /// <summary>
        /// Gets the default addresses to listen on.
        /// </summary>
        /// <param name="listen4And6">If <see langword="true" />, return both IPv6 and IPv4 "Any" addresses; if <see langword="false" />,
        /// use only IPv6 if the OS supports it or only IPv4 if not; otherwise, use a default value appropriate for the OS platform.</param>
        /// <returns>The IPv6 and/or IPv4 "any" addresses to listen on.</returns>
        /// <remarks>
        ///   <para>
        ///     On Linux, if a socket binds to an IPv6 port it automatically also binds to an associated IPv4 port. Therefore,
        ///   <paramref name="listen4And6" /> should be <see langword="false" /> (an exception will be thrown if it's not).
        ///   </para>
        ///   <para>
        ///     If <paramref name="listen4And6" /> is <see langword="null" />, it will default to <see langword="true" /> on Windows and <see langword="false" /> on Unix
        ///     (which is correct for Linux, but may not be appropriate for other Unix operating systems).
        ///   </para>
        /// </remarks>
        public static IPAddress[] GetDefaultListenerAddresses(bool? listen4And6)
        {
            if( System.Net.Sockets.Socket.OSSupportsIPv6 )
            {
                if( listen4And6 ?? true )
                    return new[] { IPAddress.IPv6Any, IPAddress.Any };
                else
                    return new[] { IPAddress.IPv6Any };
            }
            else
            {
                return new[] { IPAddress.Any };
            }
        }

        /// <summary>
        /// Starts listening for incoming connections.
        /// </summary>
        /// <remarks>
        /// Listening is done on a separate thread; this function returns immediately.
        /// </remarks>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Start()
        {
            if( _listenerThreads == null )
            {
                _listenerThreads = new Thread[_listeners.Length];
                for( int x = 0; x < _listeners.Length; ++x )
                {
                    Thread listenerThread = new Thread(Run) { Name = "TcpServer_" + _listeners[x].LocalEndpoint.ToString(), IsBackground = true };
                    _listenerThreads[x] = listenerThread;
                    listenerThread.Start(_listeners[x]);
                }
            }
        }

        /// <summary>
        /// Stops listening for incoming connections.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Stop()
        {
            if( _listenerThreads != null )
            {
                _running = false;
                foreach( TcpListener listener in _listeners )
                {
                    // On Linux, the synchronous accept won't cancel if Stop is called without calling
                    // shutdown first. On Windows, calling Shutdown throws.
                    if (Environment.OSVersion.Platform == PlatformID.Unix)
                    {
                        listener.Server.Shutdown(SocketShutdown.Both);
                    }

                    listener.Stop();
                }

                _listenerThreads = null;
            }
        }

        /// <summary>
        /// When overridden in a derived class, handles a server connection.
        /// </summary>
        /// <param name="client">A <see cref="TcpClient"/> class used to send and receive data to the client that
        /// connected to the server.</param>
        protected abstract void HandleConnection(TcpClient client);

        private void Run(object parameter)
        {
            TcpListener listener = (TcpListener)parameter;

            _running = true;
            listener.Start(Int32.MaxValue);
            _log.InfoFormat("TCP server started on address {0}.", listener.LocalEndpoint);

            while( _running )
            {
                WaitForConnections(listener);
            }
        }
        
        private void WaitForConnections(TcpListener listener)
        {
            try
            {
                // I discovered that using BeginAcceptTcpClient would call the callback immediately on the current thread
                // if there was already a connection in the queue, thus blocking the server until that connection was
                // handled. So I switch to manually creating threads.
                TcpClient client = listener.AcceptTcpClient();
                if( _maxConnections != 0 )
                {
                    int currentValue;
                    do
                    {
                        currentValue = _connections;
                        if( currentValue >= _maxConnections )
                        {
                            client.Client.Send(_connectionRejected);
                            client.Close();
                            return;
                        }
                    } while( currentValue != Interlocked.CompareExchange(ref _connections, currentValue + 1, currentValue) );

                    // If _maxConnections > 0, we need to send a value to indicate we accepted the connection.
                    client.Client.Send(_connectionAccepted);
                }
                Thread handlerThread = new Thread(ConnectionHandlerThread);
                handlerThread.IsBackground = true;
                handlerThread.Start(client);
            }
            catch( SocketException ex )
            {
                // Don't log errors when shutting down.
                if( _running )
                    _log.Error("An error occurred accepting a client connection.", ex);
            }
            catch( ObjectDisposedException )
            {
                // Only ignore when shutting down.
                if( _running )
                    throw;
            }
        }

        private void ConnectionHandlerThread(object parameter)
        {
            try
            {
                using( TcpClient client = (TcpClient)parameter )
                {
                    //_log.InfoFormat("Connection accepted from {0}.", client.Client.RemoteEndPoint);
                    HandleConnection(client);
                }
            }
            catch( SocketException ex )
            {
                _log.Error("An error occurred handling a client connection.", ex);
            }
            catch( ObjectDisposedException )
            {
                // Only ignore when shutting down.
                if( _running )
                    throw;
            }
            finally
            {
                if( _maxConnections != 0 )
                    Interlocked.Decrement(ref _connections);
            }
        }
    }
}
