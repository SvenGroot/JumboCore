// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

#pragma warning disable SYSLIB0011 // BinaryFormatter is deprecated.

namespace Ookii.Jumbo.Rpc
{
    sealed class RpcClientConnectionHandler : IDisposable
    {
        private readonly TcpClient _client;
        private readonly RpcStream _stream;
        private readonly BinaryFormatter _formatter = new BinaryFormatter();
        private readonly ServerConnectionCache _cache;
        private bool _hostNameSent;

        public RpcClientConnectionHandler(string hostName, int port, ServerConnectionCache cache)
        {
            _client = new TcpClient(hostName, port);
            //_client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
            _client.NoDelay = true;
            _stream = new RpcStream(_client);
            _cache = cache;
        }

        public object? SendRequest(string objectName, string interfaceName, string operationName, object[] parameters)
        {
            using (var stream = new MemoryStream())
            {
                if (!_hostNameSent)
                {
                    WriteString(ServerContext.LocalHostName, stream);
                    _hostNameSent = true;
                }
                WriteString(objectName, stream);
                WriteString(interfaceName, stream);
                WriteString(operationName, stream);
                if (parameters != null)
                    _formatter.Serialize(stream, parameters);
                stream.WriteTo(_stream);
            }

            var status = (RpcResponseStatus)_stream.ReadByte();
            object? result = null;
            if (status != RpcResponseStatus.SuccessNoValue)
                result = _formatter.Deserialize(_stream);

            if (status != RpcResponseStatus.Error)
                return result;
            else
            {
                throw (Exception)result!;
            }
        }

        public void ReturnToCache()
        {
            _cache.ReturnConnection(this);
        }

        public void Close()
        {
            _stream.Close();
            _client.Close();
        }

        private static void WriteString(string value, Stream stream)
        {
            var buffer = Encoding.UTF8.GetBytes(value);
            if (buffer.Length > byte.MaxValue)
                throw new ArgumentException("String is too long.");
            stream.WriteByte((byte)buffer.Length);
            stream.Write(buffer, 0, buffer.Length);
        }

        #region IDisposable Members

        public void Dispose()
        {
            _stream.Dispose();
            ((IDisposable)_client).Dispose();
        }

        #endregion
    }
}
