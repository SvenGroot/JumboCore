﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using System.Threading;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class PipelinePullTaskRecordWriter<TRecord, TPipelinedTaskOutput> : RecordWriter<TRecord>
    {
        #region Nested types

        sealed class ProducerConsumerBuffer : IDisposable
        {
            private readonly TRecord[] _buffer;
            private readonly int _bufferSize;
            private int _readPos;
            private int _writePos;
            private readonly AutoResetEvent _writePosChanged = new AutoResetEvent(false);
            private readonly AutoResetEvent _readPosChanged = new AutoResetEvent(false);
            private readonly ManualResetEvent _cancelEvent = new ManualResetEvent(false);
            private readonly WaitHandle[] _writeWaitHandles;
            private readonly WaitHandle[] _readWaitHandles;
            private volatile bool _cancelled;
            private readonly int _chunkSize;
            private volatile bool _finished;
            private bool _disposed;

            public ProducerConsumerBuffer(int bufferSize, int chunkSize)
            {
                if( bufferSize < 2 )
                    throw new ArgumentOutOfRangeException("bufferSize", "bufferSize must be larger than one.");


                _bufferSize = bufferSize;
                _chunkSize = chunkSize;
                _buffer = new TRecord[bufferSize];
                _readPos = bufferSize - 1;

                _writeWaitHandles = new WaitHandle[] { _readPosChanged, _cancelEvent };
                _readWaitHandles = new WaitHandle[] { _writePosChanged, _cancelEvent };
            }

            public bool Write(TRecord item)
            {
                _buffer[_writePos] = item;
                int newPos = (_writePos + 1) % _bufferSize;
                while( !_cancelled && newPos == _readPos )
                    WaitHandle.WaitAny(_writeWaitHandles);

                _writePos = newPos;
                if( _writePos % _chunkSize == 0 )
                    _writePosChanged.Set();
                return !_cancelled;
            }

            public bool Read(out TRecord item)
            {
                int newPos = (_readPos + 1) % _bufferSize;
                _readPos = newPos;
                if( _readPos % _chunkSize == 0 )
                    _readPosChanged.Set();
                while( !_cancelled && !_finished && _readPos == _writePos )
                {
                    WaitHandle.WaitAny(_readWaitHandles);
                }

                if( _cancelled || (_finished && _readPos == _writePos) )
                {
                    item = default(TRecord);
                    return false;
                }
                else
                {
                    item = _buffer[_readPos];
                    return true;
                }
            }

            public void Cancel()
            {
                _cancelled = true;
                _cancelEvent.Set();
            }

            public void Finish()
            {
                _finished = true;
                _cancelEvent.Set();
            }

            public void Dispose()
            {
                if( !_disposed )
                {
                    _disposed = true;
                    ((IDisposable)_cancelEvent).Dispose();
                    ((IDisposable)_readPosChanged).Dispose();
                    ((IDisposable)_writePosChanged).Dispose();
                }
                GC.SuppressFinalize(this);
            }
        }

        sealed class BufferRecordReader : RecordReader<TRecord>
        {
            // This class should NOT dispose the buffer; the record writer takes care of that.
            private readonly ProducerConsumerBuffer _buffer;

            public BufferRecordReader(ProducerConsumerBuffer buffer)
            {
                _buffer = buffer;
            }

            public override float Progress
            {
                get { return 0.0f; }
            }

            protected override bool ReadRecordInternal()
            {
                TRecord record;
                if( _buffer.Read(out record) )
                {
                    CurrentRecord = record;
                    return true;
                }
                else
                    return false;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(PipelinePullTaskRecordWriter<TRecord, TPipelinedTaskOutput>));

        private readonly TaskExecutionUtility _taskExecution;
        private readonly RecordWriter<TPipelinedTaskOutput> _output;
        private ITask<TRecord, TPipelinedTaskOutput> _task;
        private readonly TaskId _taskId;
        private Thread _taskThread;
        private ProducerConsumerBuffer _buffer;

        public PipelinePullTaskRecordWriter(TaskExecutionUtility taskExecution, RecordWriter<TPipelinedTaskOutput> output, TaskId taskId)
        {
            if( taskExecution == null )
                throw new ArgumentNullException("taskExecution");
            if( output == null )
                throw new ArgumentNullException("output");
            if( taskId == null )
                throw new ArgumentNullException("taskId");

            _taskExecution = taskExecution;
            _task = (ITask<TRecord, TPipelinedTaskOutput>)taskExecution.Task; // just to ensure the task instance gets added to additional progress sources up front.
            _output = output;
            _taskId = taskId;
        }

        public void Finish()
        {
            if( _taskThread != null )
            {
                _buffer.Finish();
                _taskThread.Join();
            }
        }

        protected override void WriteRecordInternal(TRecord record)
        {
            if( _taskThread == null )
            {
                _buffer = new ProducerConsumerBuffer(10000, 100);
                _taskThread = new Thread(TaskThread) { Name = "PipelineChannel_" + _taskId.ToString(), IsBackground = true };
                _taskThread.Start();
            }

            _buffer.Write(record);
        }

        private void TaskThread()
        {
            _task = (ITask<TRecord, TPipelinedTaskOutput>)_taskExecution.Task;
            using( BufferRecordReader reader = new BufferRecordReader(_buffer) )
            {
                _task.Run(reader, _output);
            }
            _log.Debug("Pipelined task thread has finished.");
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if( disposing )
                _buffer.Dispose();
        }
    }
}
