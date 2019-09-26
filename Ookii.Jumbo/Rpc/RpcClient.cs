// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Collections;
using System.Threading;

namespace Ookii.Jumbo.Rpc
{
    static class RpcClient
    {
        // Using Hashtable instead of generic Dictionary because Hashtable supports multiple readers without locking in the presence of a single writer.
        private static readonly Hashtable _connectionCache = new Hashtable();
        private static readonly TimeSpan _connectionTimeout = TimeSpan.FromSeconds(10); // TODO: Make this configurable.
        private static readonly WaitOrTimerCallback _timeoutCallback;
        private static readonly AutoResetEvent _timeoutEvent;
        private static RegisteredWaitHandle _registeredTimeoutEvent;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline")]
        static RpcClient()
        {
            _timeoutCallback = new WaitOrTimerCallback(TimeoutConnections);
            _timeoutEvent = new AutoResetEvent(false);
            _registeredTimeoutEvent = ThreadPool.RegisterWaitForSingleObject(_timeoutEvent, _timeoutCallback, null, _connectionTimeout, true);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Object is not owned")]
        public static object SendRequest(string hostName, int port, string objectName, string interfaceName, string operationName, object[] parameters)
        {
            // This method is public only because the dynamic assemblies must be able to access it.
            RpcClientConnectionHandler handler = GetConnection(new ServerAddress(hostName, port));
            object result = handler.SendRequest(objectName, interfaceName, operationName, parameters);
            handler.ReturnToCache();
            return result;
        }

        internal static void CloseConnections()
        {
            foreach( ServerConnectionCache cache in _connectionCache.Values )
                cache.CloseConnections();
            _connectionCache.Clear();
        }

        private static RpcClientConnectionHandler GetConnection(ServerAddress address)
        {
            ServerConnectionCache cache = (ServerConnectionCache)_connectionCache[address];
            if( cache == null )
            {
                cache = new ServerConnectionCache(_connectionTimeout);
                lock( _connectionCache )
                {
                    _connectionCache[address] = cache;
                }
            }

            RpcClientConnectionHandler handler = cache.GetConnection();
            if( handler == null )
                return new RpcClientConnectionHandler(address.HostName, address.Port, cache); // Will be added to the cache when the client is done with it.
            else
                return handler;
        }

        private static void TimeoutConnections(object state, bool wasSignalled)
        {
            DateTime now = DateTime.UtcNow;
            lock( _connectionCache )
            {
                foreach( DictionaryEntry connection in _connectionCache )
                {
                    ((ServerConnectionCache)connection.Value).TimeoutConnections(now);
                }
            }
            _registeredTimeoutEvent.Unregister(null);
            _registeredTimeoutEvent = ThreadPool.RegisterWaitForSingleObject(_timeoutEvent, _timeoutCallback, null, _connectionTimeout, true);
        }
    }
}
