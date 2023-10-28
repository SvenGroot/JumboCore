// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Ookii.Jumbo.Rpc;

static class RpcRequestHandler
{
    private record class ServerObject(object Server, Dictionary<string, IRpcDispatcher> Dispatchers);

    private static Dictionary<string, ServerObject>? _pendingRegisteredObjects = new();
    private static ImmutableDictionary<string, ServerObject>? _registeredObjects;

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
            SendError(ex, writer);
        }
    }

    public static void RegisterObject(string objectName, object server)
    {
        if (_pendingRegisteredObjects == null)
        {
            throw new InvalidOperationException("Object registration has finished.");
        }

        _pendingRegisteredObjects[objectName] = new ServerObject(server, GetDispatchers(server.GetType()));
    }

    public static void FinishRegistration()
    {
        if (_pendingRegisteredObjects != null) 
        {
            _registeredObjects = _pendingRegisteredObjects.ToImmutableDictionary();
            _pendingRegisteredObjects = null;
        }
    }

    public static void SendError(Exception exception, BinaryWriter writer)
    {
        writer.Write((byte)RpcResponseStatus.Error);
        writer.Write(exception.GetType().AssemblyQualifiedName ?? exception.GetType().Name);
        writer.Write(exception.Message);
        writer.Write(exception.StackTrace ?? "");
    }

    private static ServerObject? GetRegisteredObject(string objectName)
    {
        if (_registeredObjects == null)
        {
            throw new InvalidOperationException("RPC server registration has not yet finished.");
        }

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
