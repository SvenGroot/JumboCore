// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Ookii.Jumbo.Rpc
{
    sealed class RpcClientConnectionHandler : IDisposable
    {
        private readonly TcpClient _client;
        private readonly RpcStream _stream;
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

        public BinaryReader? SendRequest(string objectName, string interfaceName, string operationName, Action<BinaryWriter>? serializer)
        {
            Debug.Assert(!string.IsNullOrEmpty(objectName));
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                if (!_hostNameSent)
                {
                    writer.Write(ServerContext.LocalHostName);
                    _hostNameSent = true;
                }

                writer.Write(objectName);
                writer.Write(interfaceName);
                writer.Write(operationName);
                serializer?.Invoke(writer);
                writer.Flush();
                stream.WriteTo(_stream);
            }

            var reader = new BinaryReader(_stream, Encoding.UTF8, true);
            var status = (RpcResponseStatus)reader.ReadByte();
            return status switch
            {
                RpcResponseStatus.Success => reader,
                RpcResponseStatus.SuccessNoValue => null,
                RpcResponseStatus.Error => throw RpcRemoteException.ReadFrom(reader),
                _ => throw new RpcException("Malformed response.")
            };
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

        #region IDisposable Members

        public void Dispose()
        {
            _stream.Dispose();
            ((IDisposable)_client).Dispose();
        }

        #endregion
    }
}
