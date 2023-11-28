using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet;

sealed class TaskExecutionUtilityGeneric<TInput, TOutput> : TaskExecutionUtility
    where TInput : notnull
    where TOutput : notnull
{
    #region Nested types

    // This class is used if the input of a compound task is a channel and the output is a file (and there is no internal partitioning)
    // in which case we want to name output files after partitions rather than task numbers. Since there can be more than one partition,
    // this writer keeps an eye on 
    private sealed class PartitionDfsOutputRecordWriter : RecordWriter<TOutput>
    {
        private readonly TaskExecutionUtility _task;
        private readonly TaskExecutionUtility _rootTask;
        private RecordWriter<TOutput> _recordWriter;
        private readonly IMultiInputRecordReader _reader;
        private long _bytesWritten;

        public PartitionDfsOutputRecordWriter(TaskExecutionUtility task)
        {
            _task = task;
            _rootTask = task.RootTask;

            _reader = (IMultiInputRecordReader)_rootTask.InputReader!;
            _reader.CurrentPartitionChanged += new EventHandler(IMultiInputRecordReader_CurrentPartitionChanged);
            CreateOutputWriter();
        }

        public override long OutputBytes
        {
            get
            {
                if (_recordWriter == null)
                {
                    return _bytesWritten;
                }
                else
                {
                    return _bytesWritten + _recordWriter.OutputBytes;
                }
            }
        }

        protected override void WriteRecordInternal(TOutput record)
        {
            _recordWriter.WriteRecord(record);
        }

        private void IMultiInputRecordReader_CurrentPartitionChanged(object? sender, EventArgs e)
        {
            if (_recordWriter != null)
            {
                _bytesWritten += _recordWriter.OutputBytes;
                _recordWriter.Dispose();
            }

            CreateOutputWriter();
        }

        [MemberNotNull(nameof(_recordWriter))]
        private void CreateOutputWriter()
        {
            _recordWriter = (RecordWriter<TOutput>)_task.CreateDfsOutputWriter(_reader.CurrentPartition);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                if (_recordWriter != null)
                {
                    _bytesWritten += _recordWriter.OutputBytes;
                    _recordWriter.Dispose();
                }
            }
        }
    }


    #endregion

    private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(TaskExecutionUtility));

    private bool _hasTaskRun;
    private PipelinePullTaskRecordWriter<TInput, TOutput>? _pipelinePullTaskRecordWriter; // Needed to finish pipelined pull tasks.
    private PipelinePrepartitionedPushTaskRecordWriter<TInput, TOutput>? _pipelinePrepartitionedPushTaskRecordWriter; // Needed to finish pipelined prepartitioned push tasks.

    public TaskExecutionUtilityGeneric(FileSystemClient fileSystemClient, JetClient jetClient, ITaskServerUmbilicalProtocol umbilical, TaskExecutionUtility parentTask, TaskContext configuration)
        : base(fileSystemClient, jetClient, umbilical, parentTask, configuration)
    {
    }

    public override TaskMetrics RunTask()
    {
        CheckDisposed();
        if (IsAssociatedTask)
        {
            throw new InvalidOperationException("You cannot run a child task.");
        }

        if (_hasTaskRun)
        {
            throw new InvalidOperationException("This task has already been run.");
        }

        _hasTaskRun = true;

        var input = (RecordReader<TInput>?)InputReader;
        var output = (RecordWriter<TOutput>)OutputWriter;
        var taskStopwatch = new Stopwatch();

        // Ensure task object created and added to additional progress sources if needed before progress thread is started.
        var task = (ITask<TInput, TOutput>)Task;

        StartProgressThread();

        var multiInputReader = input as MultiInputRecordReader<TInput>;
        if (multiInputReader != null && InputChannels!.Count == 1 && InputChannels[0].Configuration.PartitionsPerTask > 1)
        {
            RunTaskMultipleInputPartitions(multiInputReader, output, taskStopwatch, task);
        }
        else
        {
            CallTaskRunMethod(input, output, taskStopwatch, task);
        }

        TimeSpan timeWaiting;
        var multiReader = input as MultiRecordReader<TInput>;
        if (multiReader != null)
        {
            timeWaiting = multiReader.TimeWaiting;
        }
        else
        {
            timeWaiting = TimeSpan.Zero;
        }

        _log.InfoFormat("Task finished execution, execution time: {0}s; time spent waiting for input: {1}s.", taskStopwatch.Elapsed.TotalSeconds, timeWaiting.TotalSeconds);

        var metrics = new TaskMetrics();
        FinalizeTask(metrics);

        metrics.LogMetrics();

        return metrics;
    }

    protected override IRecordWriter CreateOutputRecordWriter()
    {
        if (Context.StageConfiguration.HasDataOutput)
        {
            if (Context.StageConfiguration.InternalPartitionCount == 1)
            {
                if (!ProcessesAllInputPartitions && RootTask.InputChannels != null && RootTask.InputChannels.Count == 1 && RootTask.InputChannels[0].Configuration.PartitionsPerTask > 1)
                {
                    return new PartitionDfsOutputRecordWriter(this);
                }
            }
            return (RecordWriter<TOutput>)CreateDfsOutputWriter(Context.TaskId.TaskNumber);
        }
        else if (OutputChannel != null)
        {
            //_log.Debug("Creating output channel record writer.");
            return OutputChannel.CreateRecordWriter<TOutput>();
        }
        else
        {
            throw new InvalidOperationException("Stage must have output.");
        }
    }

    internal override IRecordWriter CreatePipelineRecordWriter(object? partitioner)
    {
        if (!IsAssociatedTask)
        {
            throw new InvalidOperationException("Can't create pipeline record writer for non-child task.");
        }

        var output = (RecordWriter<TOutput>)OutputWriter;

        WarnIfNoRecordReuse();

        var task = Task;
        var pushTask = task as PushTask<TInput, TOutput>;
        if (pushTask != null)
        {
            return new PipelinePushTaskRecordWriter<TInput, TOutput>(this, output);
        }
        else
        {
            var prepartitionedPushTask = task as PrepartitionedPushTask<TInput, TOutput>;
            if (prepartitionedPushTask != null)
            {
                var partitioner2 = (IPartitioner<TInput>)partitioner!;
                partitioner2.Partitions = Context.StageConfiguration.InternalPartitionCount;
                _pipelinePrepartitionedPushTaskRecordWriter = new PipelinePrepartitionedPushTaskRecordWriter<TInput, TOutput>(this, output, partitioner2);
                return _pipelinePrepartitionedPushTaskRecordWriter;
            }
            else
            {
                _pipelinePullTaskRecordWriter = new PipelinePullTaskRecordWriter<TInput, TOutput>(this, output, Context.TaskId);
                return _pipelinePullTaskRecordWriter;
            }
        }
    }

    private void CallTaskRunMethod(RecordReader<TInput>? input, RecordWriter<TOutput> output, Stopwatch taskStopwatch, ITask<TInput, TOutput> task)
    {
        _log.Info("Running task.");
        taskStopwatch.Start();
        task.Run(input, output);
        taskStopwatch.Stop();

        FinishTask();
    }

    protected override void RunTaskFinishMethod()
    {
        // For root tasks, Finish will be called by the ITask<TInput, TOutput>.Run method.
        if (IsAssociatedTask)
        {
            var task = Task as PushTask<TInput, TOutput>;
            if (task != null)
            {
                task.Finish((RecordWriter<TOutput>)OutputWriter);
            }
            else if (_pipelinePrepartitionedPushTaskRecordWriter != null)
            {
                _pipelinePrepartitionedPushTaskRecordWriter.Finish();
            }
            else if (_pipelinePullTaskRecordWriter != null)
            {
                _pipelinePullTaskRecordWriter.Finish();
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposing)
            {
                if (_pipelinePrepartitionedPushTaskRecordWriter != null)
                {
                    _pipelinePrepartitionedPushTaskRecordWriter.Dispose();
                }

                if (_pipelinePullTaskRecordWriter != null)
                {
                    _pipelinePullTaskRecordWriter.Dispose();
                }
            }
        }
        finally
        {
            base.Dispose(disposing);
        }
    }

    private void RunTaskMultipleInputPartitions(MultiInputRecordReader<TInput> input, RecordWriter<TOutput> output, Stopwatch taskStopwatch, ITask<TInput, TOutput> task)
    {
        if (ProcessesAllInputPartitions)
        {
            using (var partitionReader = new MultiPartitionRecordReader<TInput>(this, input))
            {
                _log.Info("Running pull task.");
                taskStopwatch.Start();
                task.Run(partitionReader, output);
                taskStopwatch.Stop();
            }
        }
        else
        {
            input.CurrentPartitionChanging += new EventHandler<CurrentPartitionChangingEventArgs>(input_CurrentPartitionChanging);
            TotalInputPartitions = input.PartitionCount;
            var firstPartition = true;
            _log.Info("Running push task.");
            do
            {
                _log.InfoFormat("Running task for partition {0}.", input.CurrentPartition);
                if (firstPartition)
                {
                    firstPartition = false;
                }
                else
                {
                    ResetForNextPartition();
                    task = (ITask<TInput, TOutput>)Task;
                }
                CallTaskRunMethod(input, output, taskStopwatch, task);
                _log.InfoFormat("Finished running task for partition {0}.", input.CurrentPartition);
                // If input.NextPartition fails we will check for additional partitions, and if we got any, we need to call input.NextPartition again.
            } while (input.NextPartition() || (GetAdditionalPartitions(input) && input.NextPartition()));
        }
    }

    private void input_CurrentPartitionChanging(object? sender, CurrentPartitionChangingEventArgs e)
    {
        e.Cancel = !NotifyStartPartitionProcessing(e.NewPartitionNumber);
    }
}
