// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Ookii.Jumbo.Rpc;

static class RpcRequestHandler
{
    private record class ServerObject(object Server, Dictionary<string, IRpcDispatcher> Dispatchers);

    private static ConcurrentDictionary<string, ServerObject> _registeredObjects = new();

    public static void HandleRequest(ServerContext context, string objectName, string interfaceName, string operationName, BinaryReader reader, BinaryWriter writer)
    {
        try
        {
            var server = GetRegisteredObject(objectName) ?? throw new RpcException($"Unknown server object {objectName}.");
            if (!server.Dispatchers.TryGetValue(interfaceName, out var dispatcher))
            {
                throw new RpcException($"Unknown interface {interfaceName}.");
            }

            ServerContext.Current = context; // Set the server context for the current thread.
            log4net.ThreadContext.Properties["ClientHostName"] = context.ClientHostName;
            dispatcher.Dispatch(operationName, server.Server, reader, writer);
            ServerContext.Current = null;
        }
        catch (Exception ex)
        {
            RpcRemoteException.WriteTo(ex, writer);
        }
    }

    public static void RegisterObject(string objectName, object server)
    {
        _registeredObjects[objectName] = new ServerObject(server, GetDispatchers(server.GetType()));
    }

    private static ServerObject? GetRegisteredObject(string objectName)
    {
        _registeredObjects.TryGetValue(objectName, out var result);
        return result;
    }

    private static Dictionary<string, IRpcDispatcher> GetDispatchers(Type type)
    {
        var result = new Dictionary<string, IRpcDispatcher>();
        foreach (var iface in type.GetInterfaces()) 
        {
            if (Attribute.IsDefined(iface, typeof(RpcInterfaceAttribute)))
            {
                var dispatcherTypeName = iface.Namespace! + ".Rpc." + iface.Name + "Dispatcher";
                var dispatcherType = iface.Assembly.GetType(dispatcherTypeName, true)!;
                result.Add(iface.AssemblyQualifiedName!, (IRpcDispatcher)Activator.CreateInstance(dispatcherType)!);
            }
        }

        return result;
    }
}
