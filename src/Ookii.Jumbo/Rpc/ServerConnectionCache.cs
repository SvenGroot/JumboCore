// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Rpc;

class ServerConnectionCache
{
    #region Nested types

    private record class CachedConnection(RpcClientConnectionHandler Handler, DateTime LastUsed)
    {
        public CachedConnection? Next { get; set; }
    }

    #endregion

    private int _connectionCount;
    private CachedConnection? _firstConnection;
    private readonly TimeSpan _connectionTimeout;

    public ServerConnectionCache(TimeSpan connectionTimeout)
    {
        _connectionTimeout = connectionTimeout;
    }

    public RpcClientConnectionHandler? GetConnection()
    {
        if (_connectionCount != 0)
        {
            lock (this)
            {
                if (_firstConnection != null)
                {
                    var handler = _firstConnection.Handler;
                    _firstConnection = _firstConnection.Next;
                    --_connectionCount;
                    return handler;
                }
            }
        }
        return null;
    }

    public void ReturnConnection(RpcClientConnectionHandler handler)
    {
        lock (this)
        {
            _firstConnection = new CachedConnection(handler, DateTime.UtcNow) { Next = _firstConnection };
            ++_connectionCount;
        }
    }

    public void CloseConnections()
    {
        lock (this)
        {
            while (_firstConnection != null)
            {
                _firstConnection.Handler.Close();
                _firstConnection = _firstConnection.Next;
                --_connectionCount;
            }
        }
    }

    public void TimeoutConnections(DateTime now)
    {
        if (_connectionCount != 0)
        {
            lock (this)
            {
                var connection = _firstConnection;
                CachedConnection? previous = null;
                while (connection != null)
                {
                    if (now - connection.LastUsed > _connectionTimeout)
                    {
                        // Connection timed out, remove it from the list
                        if (previous == null)
                        {
                            _firstConnection = connection.Next;
                        }
                        else
                        {
                            previous.Next = connection.Next;
                        }

                        --_connectionCount;
                        connection.Handler.Close(); // Close the connection
                    }
                    else
                    {
                        previous = connection;
                    }

                    connection = connection.Next;
                }
            }
        }
    }
}
