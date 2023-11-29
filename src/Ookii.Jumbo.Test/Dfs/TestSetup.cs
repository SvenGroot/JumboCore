// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics;
using NUnit.Framework;

namespace Ookii.Jumbo.Test.Dfs;

[SetUpFixture]
public class TestSetup
{
    [OneTimeSetUp]
    public void Setup()
    {
        if (Environment.GetEnvironmentVariable("JUMBO_TRACE") == "true")
        {
            Trace.Listeners.Clear();
            Trace.Listeners.Add(new ConsoleTraceListener());
            Utilities.TraceLineAndFlush("Listeners configured");
        }
    }
}
