// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Reflection;
using System.Net;

namespace Ookii.Jumbo.Rpc
{
    sealed class RpcClientConnectionHandler : IDisposable
    {
        private static readonly MethodBase _fixExceptionMethod = GetFixExceptionMethod();
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

        public object SendRequest(string objectName, string interfaceName, string operationName, object[] parameters)
        {
            using( MemoryStream stream = new MemoryStream() )
            {
                if( !_hostNameSent )
                {
                    WriteString(ServerContext.LocalHostName, stream);
                    _hostNameSent = true;
                }
                WriteString(objectName, stream);
                WriteString(interfaceName, stream);
                WriteString(operationName, stream);
                if( parameters != null )
                    _formatter.Serialize(stream, parameters);
                stream.WriteTo(_stream);
            }

            RpcResponseStatus status = (RpcResponseStatus)_stream.ReadByte();
            object result = null;
            if( status != RpcResponseStatus.SuccessNoValue )
                result = _formatter.Deserialize(_stream);

            if( status != RpcResponseStatus.Error )
                return result;
            else
            {
                // HACK: Need to depend on internal method to preserve stack trace when rethrowing the exception. Bad but there's no other way.
                if( _fixExceptionMethod == null )
                    throw (Exception)result;
                else
                    throw (Exception)_fixExceptionMethod.Invoke(result, null);
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
            byte[] buffer = Encoding.UTF8.GetBytes(value);
            if( buffer.Length > byte.MaxValue )
                throw new ArgumentException("String is too long.");
            stream.WriteByte((byte)buffer.Length);
            stream.Write(buffer, 0, buffer.Length);
        }

        private static MethodBase GetFixExceptionMethod()
        {
            string methodName;
            switch( RuntimeEnvironment.RuntimeType )
            {
            case RuntimeEnvironmentType.DotNet:
                methodName = "PrepForRemoting";
                break;
            case RuntimeEnvironmentType.Mono:
                methodName = "FixRemotingException";
                break;
            default:
                return null;
            }
            return typeof(Exception).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance);

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
