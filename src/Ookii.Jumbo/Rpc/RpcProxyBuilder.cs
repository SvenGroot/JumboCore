// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Ookii.Jumbo.Rpc
{
    static class RpcProxyBuilder
    {
        private static readonly Dictionary<Type, Type> _proxies = new();

        public static object GetProxy(Type interfaceType, string hostName, int port, string objectName)
        {
            Type? proxyType;
            lock (_proxies)
            {
                if (!_proxies.TryGetValue(interfaceType, out proxyType))
                {
                    proxyType = CreateProxy(interfaceType);
                    _proxies.Add(interfaceType, proxyType);
                }
            }

            return Activator.CreateInstance(proxyType, hostName, port, objectName)!;
        }

        // Called inside _proxies lock for thread safety.
        private static Type CreateProxy(Type interfaceType)
        {
            ArgumentNullException.ThrowIfNull(interfaceType);
            if (!interfaceType.IsInterface)
                throw new ArgumentException("Type is not an interface.", nameof(interfaceType));
            if (!Attribute.IsDefined(interfaceType, typeof(RpcInterfaceAttribute)))
                throw new ArgumentException("Type is not an RPC interface.");

            var clientTypeName = interfaceType.Namespace! + ".Rpc." + interfaceType.Name + "Client";
            return interfaceType.Assembly.GetType(clientTypeName, true)!;
        }
    }
}
