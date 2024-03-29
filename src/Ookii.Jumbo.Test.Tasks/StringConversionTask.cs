﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class StringConversionTask : ITask<Utf8String, int>
    {
        #region ITask<Utf8StringWritable,int> Members

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            foreach (var record in input.EnumerateRecords())
            {
                output.WriteRecord(Convert.ToInt32(record.ToString()));
            }
        }

        #endregion
    }
}
