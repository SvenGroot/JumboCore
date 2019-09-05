﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            if( hostName == null )
                throw new ArgumentNullException("hostName");
            if( objectName == null )
                throw new ArgumentNullException("objectName");
            if( interfaceName == null )
                throw new ArgumentNullException("interfaceName");
            if( port < 1 || port > ushort.MaxValue )
                throw new ArgumentOutOfRangeException("port");

            _hostName = hostName;
            _port = port;
            _objectName = objectName;
            _interfaceName = interfaceName;
        }

        /// <summary>
        /// Sends an RPC request. This class is for internal Jumbo use only and should not be used from your code.
        /// </summary>
        /// <param name="operationName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected object SendRequest(string operationName, object[] parameters)
        {
            return RpcClient.SendRequest(_hostName, _port, _objectName, _interfaceName, operationName, parameters);
        }
    }
}
