// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Net;

namespace Ookii.Jumbo.Rpc;

/// <summary>
/// Provides context for a server RPC call.
/// </summary>
public class ServerContext
{
    [ThreadStatic]
    private static ServerContext? _current;
    private static readonly string _localHostName = System.Net.Dns.GetHostName();

    /// <summary>
    /// Gets the currently active server context for this thread.
    /// </summary>
    public static ServerContext? Current
    {
        get { return _current; }
        internal set { _current = value; }
    }

    /// <summary>
    /// Gets the host name of the client that called the server.
    /// </summary>
    public string? ClientHostName { get; internal set; }

    /// <summary>
    /// Gets the IP address of the client that called the server.
    /// </summary>
    public IPAddress? ClientHostAddress { get; set; }

    /// <summary>
    /// Gets the name of the local host.
    /// </summary>
    /// <value>The name of the local host.</value>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1702:CompoundWordsShouldBeCasedCorrectly", MessageId = "LocalHost")]
    public static string LocalHostName
    {
        get { return _localHostName; }
    }
}
