// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet;

namespace Ookii.Jumbo.Test.Tasks
{
    [AllowRecordReuse]
    public class LineCounterTask : Configurable, ITask<Utf8String, int>
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(LineCounterTask));

        public void Run(RecordReader<Utf8String> input, RecordWriter<int> writer)
        {
            _log.Info("Running");
            int lines = 0;
            while (input.ReadRecord())
            {
                ++lines;
                TaskContext.StatusMessage = string.Format("Counted {0} lines.", lines);
            }
            _log.Info(lines);
            if (writer != null)
                writer.WriteRecord(lines);
            _log.Info("Done");
        }
    }

}
