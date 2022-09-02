// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet.Channels
{
    [Flags]
    enum TcpChannelConnectionFlags : byte
    {
        None = 0,
        FinalSegment = 1,
        KeepAlive = 2
    }
}
