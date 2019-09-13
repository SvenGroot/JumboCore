// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Ookii.Jumbo;
using Ookii.Jumbo.Jet;
using Ookii.Jumbo.Jet.Jobs;
using Ookii.Jumbo.Jet.Scheduling;

namespace JobServerApplication
{
    sealed class StageInfo : IStageInfo
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(StageInfo));

        private readonly List<TaskInfo> _tasks = new List<TaskInfo>();
        private readonly StageConfiguration _configuration;
        private List<StageInfo> _softDependentStages;
        private List<StageInfo> _hardDependentStages;
        private int _remainingTasks;
        private int _remainingSchedulingDependencies;
        private readonly float _schedulingThreshold;

        public StageInfo(JobInfo job, StageConfiguration configuration)
        {
            _configuration = configuration;
            _remainingTasks = configuration.TaskCount;

            if( job != null )
            {
                if( !configuration.TryGetSetting(JobConfiguration.SchedulingThresholdSettingKey, out _schedulingThreshold) )
                    _schedulingThreshold = job.Configuration.GetSetting(JobConfiguration.SchedulingThresholdSettingKey, JobServer.Instance.Configuration.JobServer.SchedulingThreshold);

                if( _schedulingThreshold < 0 || _schedulingThreshold > 1 )
                {
                    _log.WarnFormat("Invalid scheduling threshold {0} for stage {1}, using 0 instead.", _schedulingThreshold, configuration.StageId);
                    _schedulingThreshold = 0;
                }

                // We need to be notified if the dependency is finished if it is a hard dependency, or if it isn't ready for scheduling itself.
                foreach( StageConfiguration dependency in job.Configuration.GetExplicitDependenciesForStage(configuration.StageId) )
                {
                    StageInfo stage = job.GetStage(dependency.Root.StageId);
                    ++_remainingSchedulingDependencies;
                    if( stage._hardDependentStages == null )
                        stage._hardDependentStages = new List<StageInfo>();
                    stage._hardDependentStages.Add(this);
                }
            }
        }

        public string StageId
        {
            get { return _configuration.StageId; }
        }

        public StageConfiguration Configuration
        {
            get { return _configuration; }
        }

        public List<TaskInfo> Tasks
        {
            get { return _tasks; }
        }

        public bool IsReadyForScheduling
        {
            get { return _remainingSchedulingDependencies == 0; }
        }

        public void SetupSoftDependencies(JobInfo job)
        {
            foreach( StageConfiguration inputStage in job.Configuration.GetInputStagesForStage(_configuration.StageId) )
            {
                StageInfo stage = job.GetStage(inputStage.Root.StageId);
                // Ignore scheduling threshold for TCP channels.
                if( !stage.IsReadyForScheduling || (_schedulingThreshold > 0 && inputStage.OutputChannel.ChannelType != Ookii.Jumbo.Jet.Channels.ChannelType.Tcp) )
                {
                    ++_remainingSchedulingDependencies;
                    if( stage._softDependentStages == null )
                        stage._softDependentStages = new List<StageInfo>();
                    stage._softDependentStages.Add(this);
                }
            }
        }

        /// <summary>
        /// NOTE: Only call inside scheduler lock
        /// </summary>
        public void NotifyTaskFinished()
        {
            if( Interlocked.Decrement(ref _remainingTasks) == 0 )
                NotifyHardDependentStages();
            NotifySoftDependentStages();
        }

        public StageStatus ToStageStatus()
        {
            StageStatus result = new StageStatus() { StageId = StageId };
            result.Tasks.AddRange(from task in Tasks select task.ToTaskStatus());
            return result;
        }

        private void NotifyHardDependentStages()
        {
            if( _hardDependentStages != null )
            {
                // This can happen only once, so there's no need to remove items from the _hardDependentStages collection.
                foreach( StageInfo stage in _hardDependentStages )
                    stage.NotifyDependencyFinished(1.0f); // Hard dependencies are only notified when all tasks are finished.
            }
        }

        private void NotifySoftDependentStages()
        {
            System.Diagnostics.Debug.Assert(IsReadyForScheduling);
            float percentTasksFinished = (_tasks.Count() - _remainingTasks) / (float)_tasks.Count;
            if( _softDependentStages != null )
            {
                for( int x = 0; x < _softDependentStages.Count; ++x )
                {
                    StageInfo stage = _softDependentStages[x];
                    if( stage.NotifyDependencyFinished(percentTasksFinished) )
                    {
                        // Don't notify soft dependent stages again after they're satisfied.
                        _softDependentStages.RemoveAt(x);
                        --x;
                    }
                }
            }
        }

        private bool NotifyDependencyFinished(float percentTasksCompleted)
        {
            // This function is called if a hard dependency finishes all tasks, or a soft dependency finished a task or
            // became ready for scheduling.
            if( percentTasksCompleted >= _schedulingThreshold )
            {
                if( Interlocked.Decrement(ref _remainingSchedulingDependencies) == 0 )
                {
                    _log.InfoFormat("Stage {0} is ready for scheduling.", _configuration.StageId);
                    NotifySoftDependentStages();
                }
                return true;
            }
            return false;
        }


        IEnumerable<ITaskInfo> IStageInfo.Tasks
        {
            get { return _tasks; }
        }

        public int UnscheduledTaskCount
        {
            get { return _tasks.Where(task => !task.IsAssignedToServer).Count(); }
        }
    }
}
