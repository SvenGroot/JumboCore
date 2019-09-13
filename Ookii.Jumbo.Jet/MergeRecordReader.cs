// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Linq;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Jet.Channels;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Record reader that merges the records from multiple sorted input record readers.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <remarks>
    /// <para>
    ///   If <see cref="Channel"/> is not <see langword="null"/>, the <see cref="MergeRecordReader{T}"/> will use the <see cref="Tasks.TaskConstants.SortTaskComparerSettingKey"/>
    ///   on the <see cref="Jobs.StageConfiguration.StageSettings"/> of the input stage to determine the comparer to use. Otherwise, it will use the 
    ///   <see cref="MergeRecordReaderConstants.ComparerSetting"/> of the current stage. If neither is specified, <see cref="Comparer{T}.Default"/> will be used.
    /// </para>
    /// </remarks>
    [AdditionalProgressCounter("Sort")]
    public sealed class MergeRecordReader<T> : MultiInputRecordReader<T>, IConfigurable, IChannelMultiInputRecordReader, IHasAdditionalProgress
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(MergeRecordReader<T>));

        private int _maxDiskInputsPerMergePass;
        private float _memoryStorageTriggerLevel;
        private string _mergeIntermediateOutputPath;
        private bool _purgeMemoryBeforeFinalPass;
        private bool _disposed;
        private bool _configured;
        private IInputChannel _channel;
        private volatile bool _memoryStorageFull;

        private readonly MergeHelper<T> _mergeHelper = new MergeHelper<T>();
        private PartitionMerger<T>[] _partitionMergers;
        private readonly Dictionary<int, PartitionMerger<T>> _finalPassMergers = new Dictionary<int, PartitionMerger<T>>();
        private IEnumerator<MergeResultRecord<T>> _currentPartitionFinalPass;

        private Thread _mergeThread;
        private readonly ManualResetEvent _cancelEvent = new ManualResetEvent(false);
        private readonly AutoResetEvent _inputAddedEvent = new AutoResetEvent(false);
        private readonly object _finalPassLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="MergeRecordReader{T}"/> class.
        /// </summary>
        /// <param name="partitions">The partitions that this multi input record reader will read.</param>
        /// <param name="totalInputCount">The total number of input readers that this record reader will have.</param>
        /// <param name="allowRecordReuse"><see langword="true"/> if the record reader may reuse record instances; otherwise, <see langword="false"/>.</param>
        /// <param name="bufferSize">The buffer size to use to read input files.</param>
        /// <param name="compressionType">The compression type to us to read input files.</param>
        public MergeRecordReader(IEnumerable<int> partitions, int totalInputCount, bool allowRecordReuse, int bufferSize, CompressionType compressionType)
            : base(partitions, totalInputCount, allowRecordReuse, bufferSize, compressionType)
        {
        }

        /// <summary>
        /// Gets the combined progress of the record readers.
        /// </summary>
        /// <value>A value between 0 and 1 that indicates the overall progress of the <see cref="MultiInputRecordReader{T}"/>.</value>
        public override float Progress
        {
            get
            {
                lock( _finalPassLock )
                {
                    if( _finalPassMergers.Count == 0 )
                        return 0.0f;
                    else
                        return _finalPassMergers.Values.Average(m => m.FinalPassProgress);
                }
            }
        }

        /// <summary>
        /// Gets the additional progress value.
        /// </summary>
        /// <value>The additional progress value.</value>
        /// <remarks>
        /// This property must be thread safe.
        /// </remarks>
        public float AdditionalProgress
        {
            get 
            {
                return base.Progress;
            }
        }

        /// <summary>
        /// Gets or sets the input channel that this reader is reading from.
        /// </summary>
        /// <value>The channel.</value>
        public IInputChannel Channel
        {
            get { return _channel; }
            set
            {
                if( _channel != null )
                    _channel.MemoryStorageFull -= _channel_MemoryStorageFull;
                _channel = value;
                if( _channel != null )
                    _channel.MemoryStorageFull += _channel_MemoryStorageFull;
            }
        }

        /// <summary>
        /// Gets or sets the configuration used to access the Distributed File System.
        /// </summary>
        public Ookii.Jumbo.Dfs.DfsConfiguration DfsConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration used to access the Jet servers.
        /// </summary>
        public JetConfiguration JetConfiguration { get; set; }

        /// <summary>
        /// Gets or sets the configuration for the task attempt.
        /// </summary>
        public TaskContext TaskContext { get; set; }

        /// <summary>
        /// Gets the bytes read.
        /// </summary>
        public override long BytesRead
        {
            get
            {
                return base.BytesRead + _mergeHelper.BytesRead;
            }
        }

        internal int MaxDiskInputsPerMergePass
        {
            get { return _maxDiskInputsPerMergePass; }
        }

        internal string IntermediateOutputPath
        {
            get { return _mergeIntermediateOutputPath; }
        }

        /// <summary>
        /// Adds the specified input to be read by this record reader.
        /// </summary>
        /// <param name="partitions">The partitions for this input, in the same order as the partition list provided to the constructor.</param>
        /// <remarks>
        /// <para>
        ///   Which partitions a multi input record reader is responsible for is specified when that reader is created or
        ///   when <see cref="AssignAdditionalPartitions"/> is called. All calls to <see cref="AddInput"/> must specify those
        ///   exact same partitions, in the same order.
        /// </para>
        /// <para>
        ///   If you override this method, you must call the base class implementation.
        /// </para>
        /// </remarks>
        public override void AddInput(IList<RecordInput> partitions)
        {
            if( partitions == null )
                throw new ArgumentNullException("partitions");
            CheckDisposed();
            base.AddInput(partitions);

            for( int x = 0; x < partitions.Count; ++x )
            {
                _partitionMergers[x].AddInput(partitions[x]);
            }

            _inputAddedEvent.Set();
        }

        /// <summary>
        /// Assigns additional partitions to this record reader.
        /// </summary>
        /// <param name="newPartitions">The new partitions to assign.</param>
        /// <remarks>
        /// <para>
        ///   New partitions may only be assigned after all inputs for the existing partitions have been received.
        /// </para>
        /// </remarks>
        public override void AssignAdditionalPartitions(IList<int> newPartitions)
        {
            CheckDisposed();
            // Have to check both because _partitionMergers can be null before NotifyConfigurationChanged is called.
            lock( _finalPassLock )
            {
                if( _finalPassMergers.Count == 0 || _partitionMergers != null )
                    throw new InvalidOperationException("You cannot assign additional partitions until the final pass has started on the current partitions.");
            }

            base.AssignAdditionalPartitions(newPartitions);

            StartMergeThread(newPartitions);
        }

        /// <summary>
        /// Indicates the configuration has been changed. <see cref="JetActivator.ApplyConfiguration"/> calls this method
        /// after setting the configuration.
        /// </summary>
        public void NotifyConfigurationChanged()
        {
            if( _configured )
                throw new InvalidOperationException("MergeRecordReader is already configured.");

            _configured = true;

            _mergeIntermediateOutputPath = Path.Combine(TaskContext.LocalJobDirectory, TaskContext.TaskAttemptId.ToString());
            if( !Directory.Exists(_mergeIntermediateOutputPath) )
                Directory.CreateDirectory(_mergeIntermediateOutputPath);

            _maxDiskInputsPerMergePass = TaskContext.GetSetting(MergeRecordReaderConstants.MaxFileInputsSetting, JetConfiguration.MergeRecordReader.MaxFileInputs);
            if( _maxDiskInputsPerMergePass <= 1 )
                throw new InvalidOperationException("The maximum number of file inputs per pass must be larger than one.");

            _memoryStorageTriggerLevel = TaskContext.GetSetting(MergeRecordReaderConstants.MemoryStorageTriggerLevelSetting, JetConfiguration.MergeRecordReader.MemoryStorageTriggerLevel);
            if( _memoryStorageTriggerLevel < 0 || _memoryStorageTriggerLevel > 1 )
                throw new InvalidOperationException("The memory storage trigger level must be between 0 and 1.");

            _purgeMemoryBeforeFinalPass = TaskContext.GetSetting(MergeRecordReaderConstants.PurgeMemorySettingKey, JetConfiguration.MergeRecordReader.PurgeMemoryBeforeFinalPass);

            StartMergeThread(PartitionNumbers);
        }

        /// <summary>
        /// Reads a record.
        /// </summary>
        /// <returns><see langword="true"/> if an object was successfully read from the stream; <see langword="false"/> if the end of the stream or stream fragment was reached.</returns>
        protected override bool ReadRecordInternal()
        {
            CheckDisposed();

            // If this is not null, is can only become null if the current partition changes, and that has to happen on the same thread as ReadRecord.
            if( _currentPartitionFinalPass == null )
            {
                lock( _finalPassLock )
                {
                    while( _currentPartitionFinalPass == null )
                    {
                        Monitor.Wait(_finalPassLock);
                        CheckDisposed();
                    }
                }
            }

            bool result = _currentPartitionFinalPass.MoveNext();
            if( result )
                CurrentRecord = _currentPartitionFinalPass.Current.GetValue();
            else
                CurrentRecord = default(T);

            return result;
        }

        /// <summary>
        /// Raises the <see cref="MultiInputRecordReader{T}.CurrentPartitionChanged"/> event.
        /// </summary>
        /// <param name="e">The <see cref="System.EventArgs"/> instance containing the event data.</param>
        protected override void OnCurrentPartitionChanged(EventArgs e)
        {
            lock( _finalPassLock )
            {
                _currentPartitionFinalPass = GetCurrentPartitionFinalPass();
            }
            base.OnCurrentPartitionChanged(e);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources
        /// </summary>
        /// <param name="disposing"><see langword="true"/> to release both managed and unmanaged resources; <see langword="false"/> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            try
            {
                if( !_disposed )
                {
                    _disposed = true;
                    _cancelEvent.Set();
                    if( _mergeThread != null )
                        _mergeThread.Join();

                    // Dispose shouldn't get called on a thread different from the one that calls ReadRecord, but just to be safe.
                    lock( _finalPassLock )
                        Monitor.PulseAll(_finalPassLock);
                }
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        private void StartMergeThread(IList<int> partitionNumbers)
        {
            IComparer<T> comparer = GetComparer();

            Debug.Assert(_partitionMergers == null);
            _partitionMergers = new PartitionMerger<T>[partitionNumbers.Count];
            for( int x = 0; x < _partitionMergers.Length; ++x )
            {
                _partitionMergers[x] = new PartitionMerger<T>(this, partitionNumbers[x], comparer);
            }

            _mergeThread = new Thread(MergeThread)
            {
                Name = "MergeRecordReader.BackgroundMergeThread",
                IsBackground = true
            };
            _mergeThread.Start();
        }

        private void MergeThread()
        {
            _log.InfoFormat("Background merge thread started with trigger level {1} and max {2} disk inputs per pass.", TotalInputCount, _memoryStorageTriggerLevel, _maxDiskInputsPerMergePass);

            WaitHandle[] events = new WaitHandle[] { _inputAddedEvent, _cancelEvent };

            while( CurrentInputCount < TotalInputCount )
            {
                if( Channel != null && Channel.UsesMemoryStorage && (_memoryStorageFull || Channel.MemoryStorageLevel >= _memoryStorageTriggerLevel) )
                {
                    foreach( PartitionMerger<T> merger in _partitionMergers )
                        merger.RunMemoryPurgePass(_mergeHelper);
                    _memoryStorageFull = false;
                }

                foreach( PartitionMerger<T> merger in _partitionMergers )
                    merger.RunDiskMergePassIfNeeded(_mergeHelper);

                if( WaitHandle.WaitAny(events) == 1 )
                    break;
            }

            if( _cancelEvent.WaitOne(0) )
                _log.Info("Background merger was cancelled.");
            else
            {
                if( _purgeMemoryBeforeFinalPass )
                {
                    foreach( PartitionMerger<T> merger in _partitionMergers )
                        merger.RunMemoryPurgePass(_mergeHelper);
                }

                _log.Info("Preparing final merge");

                foreach( PartitionMerger<T> merger in _partitionMergers )
                    merger.PrepareFinalPass(_mergeHelper);

                _log.Info("All partitions are ready for the final pass.");

                lock( _finalPassLock )
                {
                    foreach( PartitionMerger<T> merger in _partitionMergers )
                        _finalPassMergers.Add(merger.PartitionNumber, merger);

                    // If it's already set, this is an additional set of partitions we're working on and the previous set hasn't finished processing yet.
                    if( _currentPartitionFinalPass == null )
                        _currentPartitionFinalPass = GetCurrentPartitionFinalPass();

                    // This indicates that we're ready to receive new partitions
                    _partitionMergers = null;

                    HasRecords = true;

                    Monitor.PulseAll(_finalPassLock);
                }
            }
        }

        private IComparer<T> GetComparer()
        {
            string comparerTypeName = TaskContext.StageConfiguration.GetSetting(MergeRecordReaderConstants.ComparerSetting, null);
            if( comparerTypeName == null && !(Channel == null || Channel.InputStage == null) )
            {
                if( Ookii.Jumbo.Jet.Jobs.SettingsDictionary.GetJobOrStageSetting(TaskContext.JobConfiguration, Channel.InputStage, JumboSettings.FileChannel.StageOrJob.ChannelOutputType, FileChannelOutputType.Spill) == FileChannelOutputType.SortSpill )
                    comparerTypeName = Channel.InputStage.GetSetting(JumboSettings.FileChannel.Stage.SpillSortComparerType, null);
                else
                    comparerTypeName = Channel.InputStage.GetSetting(Tasks.TaskConstants.SortTaskComparerSettingKey, null);
            }

            if( !string.IsNullOrEmpty(comparerTypeName) )
            {
                _log.DebugFormat("Using specified comparer {0}.", comparerTypeName);
                return (IComparer<T>)JetActivator.CreateInstance(Type.GetType(comparerTypeName, true), DfsConfiguration, JetConfiguration, TaskContext);
            }
            else
            {
                _log.DebugFormat("Using the default comparer for type {0}.", typeof(T));
                return null;
            }
        }

        private IEnumerator<MergeResultRecord<T>> GetCurrentPartitionFinalPass()
        {
            PartitionMerger<T> finalPassMerger;
            if( _finalPassMergers != null && _finalPassMergers.TryGetValue(CurrentPartition, out finalPassMerger) )
            {
                return finalPassMerger.FinalPassResult.GetEnumerator();
            }
            return null;
        }
        
        private void _channel_MemoryStorageFull(object sender, MemoryStorageFullEventArgs e)
        {
            e.CancelWaiting = false;
            _memoryStorageFull = true;
            _inputAddedEvent.Set();
        }
    }
}
