// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Net.Sockets;
using System.Net;
using System.Threading;

namespace Ookii.Jumbo.Rpc
{
    /// <summary>
    /// Provides functionality for registering remoting channels and services.
    /// </summary>
    public static class RpcHelper
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(RpcHelper));

        private static Dictionary<int, RpcServer> _serverChannels;
        private static volatile bool _abortRetries;

        /// <summary>
        /// Registers the server channels.
        /// </summary>
        /// <param name="port">The port on which to listen.</param>
        /// <param name="listen4And6">When IPv6 is available, <see langword="true"/> to listen on IPv4 as well as 
        /// IPv6; <see langword="false"/> to listen on IPv6 only. When IPv6 is not available, this parameter has no effect.</param>
        public static void RegisterServerChannels(int port, bool? listen4And6)
        {
            if( _serverChannels == null )
                _serverChannels = new Dictionary<int, RpcServer>();

            if( !_serverChannels.ContainsKey(port) )
            {
                IPAddress[] localAddresses = TcpServer.GetDefaultListenerAddresses(listen4And6);

                RpcServer server = new RpcServer(localAddresses, port);
                server.StartListening();

                _serverChannels.Add(port, server);
            }
        }

        /// <summary>
        /// Unregisters the server channels.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public static void UnregisterServerChannels(int port)
        {
            if( _serverChannels != null )
            {
                RpcServer server;
                if( _serverChannels.TryGetValue(port, out server) )
                {
                    server.StopListening();
                    _serverChannels.Remove(port);
                }
            }
        }

        /// <summary>
        /// Registers an object as a well-known service.
        /// </summary>
        /// <param name="objectName">The object name of the service.</param>
        /// <param name="server">The object implementing the service.</param>
        public static void RegisterService(string objectName, object server)
        {
            RpcRequestHandler.RegisterObject(objectName, server);
        }

        /// <summary>
        /// Creates a client for the specified RPC service.
        /// </summary>
        /// <typeparam name="T">The type of the RPC interface.</typeparam>
        /// <param name="hostName">The host name of the RPC server.</param>
        /// <param name="port">The port of the RPC server.</param>
        /// <param name="objectName">The object name of the service.</param>
        /// <returns>An object that implements the specified interface that forwards all calls to the specified service.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static T CreateClient<T>(string hostName, int port, string objectName)
        {
            if( hostName == null )
                throw new ArgumentNullException("hostName");
            if( port < 0 )
                throw new ArgumentOutOfRangeException("port");
            if( objectName == null )
                throw new ArgumentNullException("objectName");

            return (T)RpcProxyBuilder.GetProxy(typeof(T), hostName, port, objectName);
        }

        /// <summary>
        /// Tries to execute a remoting calls, and retries it if a network failure occurs.
        /// </summary>
        /// <param name="remotingAction">The <see cref="Action"/> that performs the remoting call.</param>
        /// <param name="retryInterval">The amount of time to wait, in milliseconds, before retrying after a failure.</param>
        /// <param name="maxRetries">The maximum amount of times to retry, or -1 to retry indefinitely.</param>
        public static void TryRemotingCall(Action remotingAction, int retryInterval, int maxRetries)
        {
            // TODO: This should be integrated into the RPC infrastructure.
            if( remotingAction == null )
                throw new ArgumentNullException("remotingAction");
            if( retryInterval <= 0 )
                throw new ArgumentOutOfRangeException("retryInterval", "The retry interval must be greater than zero.");

            bool retry = true;
            do
            {
                try
                {
                    remotingAction();
                    retry = false;
                }
                catch( RpcException ex )
                {
                    if( !_abortRetries && (maxRetries == -1 || maxRetries > 0) )
                    {
                        _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "An error occurred performing a remoting operation. Retrying in {0}.", retryInterval), ex);
                        --maxRetries;
                        Thread.Sleep(retryInterval);
                    }
                    else
                    {
                        _log.Error("An error occurred performing a remoting operation.", ex);
                        throw;
                    }
                }
                catch( System.Net.Sockets.SocketException ex )
                {
                    if( !_abortRetries && (maxRetries == -1 || maxRetries > 0) )
                    {
                        _log.Error(string.Format(System.Globalization.CultureInfo.InvariantCulture, "An error occurred performing a remoting operation. Retrying in {0}.", retryInterval), ex);
                        if( maxRetries > 0 )
                            --maxRetries;
                        Thread.Sleep(retryInterval);
                    }
                    else
                    {
                        _log.Error("An error occurred performing a remoting operation.", ex);
                        throw;
                    }
                }
            } while( retry );
        }

        /// <summary>
        /// Aborts any retry attempts done by <see cref="TryRemotingCall"/>.
        /// </summary>
        /// <remarks>
        /// All future calls to <see cref="TryRemotingCall"/> will not do any more retries, so only use this
        /// function when you are shutting down.
        /// </remarks>
        public static void AbortRetries()
        {
            _abortRetries = true;
        }

        /// <summary>
        /// Closes all RPC client connections that are not currently being used.
        /// </summary>
        public static void CloseConnections()
        {
            RpcClient.CloseConnections();
        }
    }
}
