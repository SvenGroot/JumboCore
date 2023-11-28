// Copyright (c) Sven Groot (Ookii.org)

namespace NameServerApplication;

enum FileSystemMutation
{
    Invalid = 0,
    CreateDirectory = 1,
    CreateFile = 2,
    AppendBlock = 3,
    CommitBlock = 4,
    CommitFile = 5,
    Delete = 6,
    Move = 7,
    MaxValue = Move
}
