// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class PipelinePushTaskRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
    {
        private readonly TaskExecutionUtility _taskExecution;
        private PushTask<TRecord, TPipelinedTaskOutput> _task;
        private RecordWriter<TPipelinedTaskOutput> _output;

        public PipelinePushTaskRecordWriter(TaskExecutionUtility taskExecution, RecordWriter<TPipelinedTaskOutput> output)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            if( output == null )
                throw new ArgumentNullException("output");

            _taskExecution = taskExecution;
            _task = (PushTask<TRecord, TPipelinedTaskOutput>)taskExecution.Task;
            _taskExecution.TaskInstanceCreated += new EventHandler(_taskExecution_TaskInstanceCreated);
            _output = output;
        }

        protected override void WriteRecordInternal(TRecord record)
        {
            if( _output == null )
                throw new ObjectDisposedException(typeof(PipelinePushTaskRecordWriter<TRecord, TPipelinedTaskOutput>).FullName);
            _task.ProcessRecord(record, _output);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if( disposing )
                {
                    if( _output != null )
                    {
                        _output.Dispose();
                        _output = null;
                    }
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        void _taskExecution_TaskInstanceCreated(object sender, EventArgs e)
        {
            _task = (PushTask<TRecord, TPipelinedTaskOutput>)_taskExecution.Task;
        }

    }

}
