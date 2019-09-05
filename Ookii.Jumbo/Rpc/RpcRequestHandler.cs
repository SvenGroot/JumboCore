﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Threading;

namespace Ookii.Jumbo.Rpc
{
    static class RpcRequestHandler
    {
        private class ServerObject
        {
            public object Server { get; set; }
            public Type[] Interfaces { get; set; }
        }

        private static readonly Dictionary<string, ServerObject> _registeredObjects = new Dictionary<string, ServerObject>();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static void HandleRequest(ServerContext context, string objectName, string interfaceName, string operationName, RpcServerConnectionHandler handler)
        {
            ServerObject server = GetRegisteredObject(objectName);
            if( server == null )
                handler.SendError(new RpcException("Unknown server object."));

            try
            {
                Type serverType = FindInterface(server, interfaceName);
                MethodInfo method = serverType.GetMethod(operationName, BindingFlags.Public | BindingFlags.Instance);
                if( method == null )
                    handler.SendError(new RpcException("Unknown operation."));
                object[] parameters = null;
                if( method.GetParameters().Length > 0 )
                    parameters = handler.ReadParameters();
                ServerContext.Current = context; // Set the server context for the current thread.
                log4net.ThreadContext.Properties["ClientHostName"] = context.ClientHostName;
                object result = method.Invoke(server.Server, parameters);
                ServerContext.Current = null;
                handler.SendResult(result);
            }
            catch( TargetInvocationException ex )
            {
                handler.SendError(ex.InnerException);
            }
            catch( Exception ex )
            {
                handler.SendError(ex);
            }
        }

        public static void RegisterObject(string objectName, object server)
        {
            lock( _registeredObjects )
            {
                _registeredObjects[objectName] = new ServerObject() { Server = server, Interfaces = server.GetType().GetInterfaces() };
            }
        }

        private static ServerObject GetRegisteredObject(string objectName)
        {
            ServerObject result;
            lock( _registeredObjects )
            {
                _registeredObjects.TryGetValue(objectName, out result);
            }
            return result;
        }

        private static Type FindInterface(ServerObject server, string assemblyQualifiedName)
        {
            Type[] interfaces = server.Interfaces;
            foreach( Type interfaceType in interfaces )
            {
                if( interfaceType.AssemblyQualifiedName == assemblyQualifiedName )
                    return interfaceType;
            }
            throw new RpcException("Unknown interface.");
        }
    }
}
