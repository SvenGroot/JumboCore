// Copyright (c) Sven Groot (Ookii.org)
using System.IO;
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
