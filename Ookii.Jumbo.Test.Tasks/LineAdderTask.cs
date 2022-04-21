// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class LineAdderTask : Configurable, ITask<int, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineAdderTask));

        #region ITask<int,int> Members

        public void Run(RecordReader<int> input, RecordWriter<int> output)
        {
            _log.InfoFormat("Running, input = {0}, output = {1}", input, output);
            int totalLines = 0;
            foreach (int value in input.EnumerateRecords())
            {
                totalLines += value;
                TaskContext.StatusMessage = string.Format("Counted {0} lines", totalLines);
                _log.Info(value);
            }
            _log.InfoFormat("Total: {0}", totalLines);
            output.WriteRecord(totalLines);
        }

        #endregion
    }
}
