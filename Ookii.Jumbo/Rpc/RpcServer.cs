// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Net;
using System.Net.Sockets;

namespace Ookii.Jumbo.Rpc
{
    class RpcServer
    {
        private readonly TcpListener[] _listeners;
        private readonly AsyncCallback _acceptSocketCallback;
        private volatile bool _isListening;

        public RpcServer(IPAddress[] localAddresses, int port)
        {
            if (localAddresses == null)
                throw new ArgumentNullException(nameof(localAddresses));
            if (localAddresses.Length == 0)
                throw new ArgumentException("You must specify a local address to listen on.");

            _listeners = new TcpListener[localAddresses.Length];
            for (var x = 0; x < localAddresses.Length; ++x)
                _listeners[x] = new TcpListener(localAddresses[x], port);
            _acceptSocketCallback = new AsyncCallback(AcceptSocketCallback);
        }

        public void StartListening()
        {
            foreach (var listener in _listeners)
            {
                listener.Start(Int32.MaxValue);
                listener.BeginAcceptSocket(_acceptSocketCallback, listener);
            }
            _isListening = true;
        }

        public void StopListening()
        {
            _isListening = false;
            foreach (var listener in _listeners)
            {
                listener.Stop();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void AcceptSocketCallback(IAsyncResult ar)
        {
            var listener = (TcpListener)ar.AsyncState;
            if (_isListening)
                listener.BeginAcceptSocket(_acceptSocketCallback, listener);

            Socket socket = null;
            RpcServerConnectionHandler handler = null;
            try
            {
                socket = listener.EndAcceptSocket(ar);
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, 3));
                socket.NoDelay = true;
                handler = new RpcServerConnectionHandler(socket);
                handler.BeginReadRequest();
            }
            catch (Exception ex)
            {
                if (handler != null)
                {
                    handler.TrySendError(ex);
                    handler.Dispose();
                }
                if (socket != null)
                    socket.Close();
            }
        }
    }
}
