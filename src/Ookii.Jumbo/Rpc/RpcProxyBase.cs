// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Rpc
{
    /// <summary>
    /// Abstract base class for RPC proxy classes. This class is for internal Jumbo use only and should not be used from your code.
    /// </summary>
    public abstract class RpcProxyBase
    {
        private readonly string _hostName;
        private readonly int _port;
        private readonly string _objectName;
        private readonly string _interfaceName;

        /// <summary>
        /// Initializes a new instance of the <see cref="RpcProxyBase"/> class. This class is for internal Jumbo use only and should not be used from your code.
        /// </summary>
        /// <param name="hostName"></param>
        /// <param name="port"></param>
        /// <param name="objectName"></param>
        /// <param name="interfaceName"></param>
        protected RpcProxyBase(string hostName, int port, string objectName, string interfaceName)
        {
            ArgumentNullException.ThrowIfNull(hostName);
            ArgumentNullException.ThrowIfNull(objectName);
            ArgumentNullException.ThrowIfNull(interfaceName);
            if (port < 1 || port > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(port));

            _hostName = hostName;
            _port = port;
            _objectName = objectName;
            _interfaceName = interfaceName;
        }

        /// <summary>
        /// Sends an RPC request. This class is for internal Jumbo use only and should not be used from your code.
        /// </summary>
        /// <param name="operationName">The name of the operation to invoke.</param>
        /// <param name="parameters">The parameters of the operation.</param>
        /// <returns>The result of the operation.</returns>
        protected object SendRequest(string operationName, object[] parameters)
        {
            return RpcClient.SendRequest(_hostName, _port, _objectName, _interfaceName, operationName, parameters);
        }
    }
}
