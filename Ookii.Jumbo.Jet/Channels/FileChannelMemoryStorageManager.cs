// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Ookii.Jumbo.Jet.Channels
{
    sealed class FileChannelMemoryStorageManager : IDisposable
    {
        #region Nested types

        public sealed class Reservation : IDisposable
        {
            private readonly FileChannelMemoryStorageManager _manager;
            private readonly bool _waited;


            public Reservation(FileChannelMemoryStorageManager manager, long size, bool waited)
            {
                ArgumentNullException.ThrowIfNull(manager);
                _manager = manager;
                _waited = waited;
                Size = size;
            }

            public long Size { get; private set; }

            public bool Waited
            {
                get { return _waited; }
            }

            public UnmanagedBufferMemoryStream CreateStream(long size)
            {
                if (size > Size)
                    throw new ArgumentOutOfRangeException(nameof(size), "Stream size exceeds reservation.");

                UnmanagedBufferMemoryStream stream = null;
                try
                {
                    stream = new UnmanagedBufferMemoryStream(size);
                    _manager.RegisterStream(stream);

                    Size -= size;
                    return stream;
                }
                catch
                {
                    if (stream != null)
                        stream.Dispose();
                    throw;
                }
            }

            public void Dispose()
            {
                _manager.CancelReservation(this);
                Size = 0;
            }
        }

        #endregion

        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FileChannelMemoryStorageManager));

        private const double _maxSingleStreamFraction = 0.25;

        private static FileChannelMemoryStorageManager _instance;
        private readonly long _maxSize;
        private readonly List<UnmanagedBufferMemoryStream> _inputs = new List<UnmanagedBufferMemoryStream>();
        private readonly long _maxSingleStreamSize;
        private long _currentSize;
        private bool _disposed;

        public event EventHandler StreamRemoved;
        public event EventHandler<MemoryStorageFullEventArgs> WaitingForBuffer;

        private FileChannelMemoryStorageManager(long maxSize)
        {
            if (maxSize < 0)
                throw new ArgumentOutOfRangeException(nameof(maxSize), "Memory storage size must be larger than zero.");
            _maxSize = maxSize;
            _log.InfoFormat("Created memory storage with maximum size {0}.", maxSize);
            _maxSingleStreamSize = (long)(maxSize * _maxSingleStreamFraction);
        }

        public float Level
        {
            get
            {
                lock (_inputs)
                {
                    if (_maxSize == 0L)
                        return 0f;
                    return _currentSize / (float)_maxSize;
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public static FileChannelMemoryStorageManager GetInstance(long maxSize)
        {
            if (_instance == null)
                _instance = new FileChannelMemoryStorageManager(maxSize);
            else if (_instance._maxSize != maxSize)
                _log.WarnFormat("A memory storage manager with a different max size ({0}) than the existing manager was requested; using the original size ({1}).", maxSize, _instance._maxSize);
            return _instance;
        }

        public Reservation WaitForSpaceAndReserve(long size, IDisposable disposeOnWait, int millisecondsTimeout)
        {
            CheckDisposed();
            if (size > _maxSingleStreamSize)
                return null;

            var waited = false;
            lock (_inputs)
            {
                while (_currentSize + size > _maxSize)
                {
                    if (!waited)
                    {
                        _log.Info("Waiting for buffer space...");
                        var e = new MemoryStorageFullEventArgs(_currentSize + size - _maxSize);
                        OnWaitingForBuffer(e);
                        if (e.CancelWaiting)
                            return null;
                        if (disposeOnWait != null)
                            disposeOnWait.Dispose();
                    }
                    waited = true;
                    if (!Monitor.Wait(_inputs, millisecondsTimeout))
                    {
                        _log.Warn("Waiting for buffer space timed out.");
                        return null;
                    }
                }
                if (waited)
                    _log.Info("Buffer space available");

                _currentSize += size;
                //_log.DebugFormat("Added stream of size {0} to memory storage; space used now {1}.", size, _currentSize);
                return new Reservation(this, size, waited);
            }
        }

        private void OnStreamRemoved(EventArgs e)
        {
            var handler = StreamRemoved;
            if (handler != null)
                handler(this, e);
        }

        private void OnWaitingForBuffer(MemoryStorageFullEventArgs e)
        {
            var handler = WaitingForBuffer;
            if (handler != null)
                handler(this, e);
        }

        private void RemoveStream(UnmanagedBufferMemoryStream stream)
        {
            lock (_inputs)
            {
                if (_inputs.Remove(stream))
                {
                    _currentSize -= stream.InitialCapacity;
                    //_log.DebugFormat("Removed stream from memory storage, space used now {0}.", _currentSize);
                    OnStreamRemoved(EventArgs.Empty);
                }
                else
                {
                    _log.Warn("Attempt to remove a stream that was not registered.");
                }
                Monitor.PulseAll(_inputs);
            }
        }

        private void RegisterStream(UnmanagedBufferMemoryStream stream)
        {
            lock (_inputs)
            {
                stream.Disposed += UnmanagedBufferMemoryStream_Disposed;
                _inputs.Add(stream);
            }
        }

        private void CancelReservation(Reservation reservation)
        {
            lock (_inputs)
            {
                _currentSize -= reservation.Size;
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(typeof(FileChannelMemoryStorageManager).FullName);
        }

        private void UnmanagedBufferMemoryStream_Disposed(object sender, EventArgs e)
        {
            RemoveStream((UnmanagedBufferMemoryStream)sender);
        }

        #region IDisposable Members

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                lock (_inputs)
                {
                    foreach (var stream in _inputs)
                    {
                        stream.Dispose();
                    }
                    _inputs.Clear();
                }
            }
        }

        #endregion
    }
}
