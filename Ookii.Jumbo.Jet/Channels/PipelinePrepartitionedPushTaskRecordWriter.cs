// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class PipelinePrepartitionedPushTaskRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
    {
        private readonly TaskExecutionUtility _taskExecution;
        private PrepartitionedPushTask<TRecord, TPipelinedTaskOutput> _task;
        private readonly IPartitioner<TRecord> _partitioner;
        private PrepartitionedRecordWriter<TPipelinedTaskOutput> _output;

        public PipelinePrepartitionedPushTaskRecordWriter(TaskExecutionUtility taskExecution, RecordWriter<TPipelinedTaskOutput> output, IPartitioner<TRecord> partitioner)
        {
            ArgumentNullException.ThrowIfNull(output);
            ArgumentNullException.ThrowIfNull(partitioner);

            _taskExecution = taskExecution;
            _task = (PrepartitionedPushTask<TRecord, TPipelinedTaskOutput>)_taskExecution.Task;
            _taskExecution.TaskInstanceCreated += new EventHandler(_taskExecution_TaskInstanceCreated);
            _output = new PrepartitionedRecordWriter<TPipelinedTaskOutput>(output, true);
            _partitioner = partitioner;
        }

        public void Finish()
        {
            _task.Finish(_output);
        }

        protected override void WriteRecordInternal(TRecord record)
        {
            _task.ProcessRecord(record, _partitioner.GetPartition(record), _output);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                {
                    if (_output != null)
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

        private void _taskExecution_TaskInstanceCreated(object sender, EventArgs e)
        {
            _task = (PrepartitionedPushTask<TRecord, TPipelinedTaskOutput>)_taskExecution.Task;
        }
    }


}
