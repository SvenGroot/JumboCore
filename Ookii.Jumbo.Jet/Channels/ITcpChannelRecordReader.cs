// Copyright (c) Sven Groot (Ookii.org)
using System.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    interface ITcpChannelRecordReader
    {
        void AddSegment(int size, int number, Stream stream);
        void CompleteAdding();
    }
}
