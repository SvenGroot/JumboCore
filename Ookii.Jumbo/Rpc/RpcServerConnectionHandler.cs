// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Net;

namespace Ookii.Jumbo.Rpc
{
    sealed class RpcServerConnectionHandler : IDisposable
    {
        private readonly Socket _serverSocket;
        private readonly RpcStream _stream;
        private readonly AsyncCallback _beginReadRequestCallback;
        private readonly BinaryFormatter _formatter;
        private readonly ServerContext _context;
        private bool _hostNameReceived;

        public RpcServerConnectionHandler(Socket serverSocket)
        {
            if( serverSocket == null )
                throw new ArgumentNullException(nameof(serverSocket));

            _serverSocket = serverSocket;
            _stream = new RpcStream(_serverSocket);
            _beginReadRequestCallback = new AsyncCallback(BeginReadRequestCallback);
            _formatter = new BinaryFormatter();
            _context = new ServerContext() { ClientHostAddress = ((IPEndPoint)_serverSocket.RemoteEndPoint).Address };
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void BeginReadRequest()
        {
            bool hasData = false;
            try
            {
                if( !_stream.HasData )
                    _stream.BeginBuffering(_beginReadRequestCallback);
                else
                    hasData = true;
            }
            catch( Exception ex )
            {
                CloseOnError(ex);
            }

            if( hasData )
            {
                ProcessRequest();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void BeginReadRequestCallback(IAsyncResult ar)
        {
            bool hasData = false;
            try
            {
                _stream.EndBuffering(ar);
                if( !_stream.HasData )
                    Close();
                else
                    hasData = true;
            }
            catch( Exception ex )
            {
                CloseOnError(ex);
            }

            if( hasData )
            {
                ProcessRequest();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void ProcessRequest()
        {
            try
            {
                if( !_hostNameReceived )
                {
                    _context.ClientHostName = _stream.ReadString();
                    _hostNameReceived = true;
                }
                string objectName = _stream.ReadString();
                string interfaceName = _stream.ReadString();
                string operationName = _stream.ReadString();

                RpcRequestHandler.HandleRequest(_context, objectName, interfaceName, operationName, this);
            }
            catch( Exception ex )
            {
                TrySendError(ex);
            }

            BeginReadRequest();
        }

        public object[] ReadParameters()
        {
            return (object[])_formatter.Deserialize(_stream);
        }

        public void SendResult(object obj)
        {
            SendResponse(true, obj);
        }

        public void SendError(Exception ex)
        {
            SendResponse(false, ex);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public void TrySendError(Exception ex)
        {
            try
            {
                SendError(ex);
            }
            catch
            {
            }
        }

        private void SendResponse(bool success, object response)
        {
            using( MemoryStream contentStream = new MemoryStream() )
            {
                RpcResponseStatus status = success ? (response == null ? RpcResponseStatus.SuccessNoValue : RpcResponseStatus.Success) : RpcResponseStatus.Error;
                contentStream.WriteByte((byte)status);
                if( response != null )
                    _formatter.Serialize(contentStream, response);
                contentStream.WriteTo(_stream);
            }
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
