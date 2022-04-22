// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// This class provides helper stuff for record files.
    /// </summary>
    static class RecordFile
    {
        public const byte CurrentVersion = 1;
        public const int RecordMarkerSize = 16;
        public const int RecordMarkerInterval = 2000;
        public const int RecordMarkerPrefix = -1;
        public const int RecordPrefix = 0;
    }
}
