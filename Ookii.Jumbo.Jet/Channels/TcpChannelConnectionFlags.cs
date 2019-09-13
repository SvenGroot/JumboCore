// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
