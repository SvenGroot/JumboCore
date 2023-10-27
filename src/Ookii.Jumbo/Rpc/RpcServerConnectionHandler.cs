// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Ookii.Jumbo.Rpc
{
    sealed class RpcServerConnectionHandler : IDisposable
    {
        private readonly Socket _serverSocket;
        private readonly RpcStream _stream;
        private readonly AsyncCallback _beginReadRequestCallback;
        private readonly ServerContext _context;
        private bool _hostNameReceived;

        public RpcServerConnectionHandler(Socket serverSocket)
        {
            ArgumentNullException.ThrowIfNull(serverSocket);

            _serverSocket = serverSocket;
            _stream = new RpcStream(_serverSocket);
            _beginReadRequestCallback = new AsyncCallback(BeginReadRequestCallback);
            _context = new ServerContext() { ClientHostAddress = ((IPEndPoint?)_serverSocket.RemoteEndPoint)?.Address };
        }

        public void BeginReadRequest()
        {
            var hasData = false;
            try
            {
                if (!_stream.HasData)
                    _stream.BeginBuffering(_beginReadRequestCallback);
                else
                    hasData = true;
            }
            catch (Exception ex)
            {
                CloseOnError(ex);
            }

            if (hasData)
            {
                ProcessRequest();
            }
        }

        private void BeginReadRequestCallback(IAsyncResult ar)
        {
            var hasData = false;
            try
            {
                _stream.EndBuffering(ar);
                if (!_stream.HasData)
                    Close();
                else
                    hasData = true;
            }
            catch (Exception ex)
            {
                CloseOnError(ex);
            }

            if (hasData)
            {
                ProcessRequest();
            }
        }

        public void ProcessRequest()
        {
            try
            {
                using var reader = new BinaryReader(_stream, Encoding.UTF8, true);
                using var writer = new BinaryWriter(_stream, Encoding.UTF8, true);
                if (!_hostNameReceived)
                {
                    _context.ClientHostName = reader.ReadString();
                    _hostNameReceived = true;
                }

                var objectName = reader.ReadString();
                var interfaceName = reader.ReadString();
                var operationName = reader.ReadString();
                RpcRequestHandler.HandleRequest(_context, objectName, interfaceName, operationName, reader, writer);
            }
            catch (Exception ex)
            {
                TrySendError(ex);
            }

            BeginReadRequest();
        }

        public void TrySendError(Exception ex)
        {
            try
            {
                using var writer = new BinaryWriter(_stream, Encoding.UTF8, true);
                RpcRequestHandler.SendError(ex, writer);
            }
            catch { }
        }

        private void CloseOnError(Exception ex)
        {
            TrySendError(ex);
            Close();
        }

        private void Close()
        {
            _stream.Dispose();
            _serverSocket.Close();
        }

        #region IDisposable Members

        public void Dispose()
        {
            _stream.Dispose();
            ((IDisposable)_serverSocket).Dispose();
        }

        #endregion
    }
}
