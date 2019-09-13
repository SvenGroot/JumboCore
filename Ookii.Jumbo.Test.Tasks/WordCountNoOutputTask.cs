using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;
// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Test.Tasks
{
    public class WordCountNoOutputTask : ITask<Utf8String, Pair<Utf8String, int>>
    {
        public void Run(RecordReader<Utf8String> input, RecordWriter<Pair<Utf8String, int>> output)
        {
            foreach( var record in input.EnumerateRecords() )
            {
                // No output
            }
        }
    }
}
