// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs;

namespace DataServerApplication
{
    static class ExtensionMethods
    {
        public static void WriteResult(this BinaryWriter writer, DataServerClientProtocolResult result)
        {
            writer.Write((short)result);
            writer.Flush();
        }
    }
}
