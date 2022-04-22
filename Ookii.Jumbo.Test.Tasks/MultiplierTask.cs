// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class MultiplierTask : Configurable, ITask<Utf8String, int>
    {
        #region ITask<Utf8StringWritable,int> Members

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> output)
        {
            int factor = TaskContext.JobConfiguration.GetSetting("factor", 0);

            foreach (Utf8String record in input.EnumerateRecords())
            {
                int value = Convert.ToInt32(record.ToString());
                output.WriteRecord(value * factor);
            }
        }

        #endregion
    }
}
