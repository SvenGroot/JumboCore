// $Id$
//
using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about the currently running job.
    /// </summary>
    [Serializable]
    public class JobStatus
    {
        private readonly ExtendedCollection<TaskStatus> _failedTaskAttempts = new ExtendedCollection<TaskStatus>();
        private readonly ExtendedCollection<StageStatus> _stages = new ExtendedCollection<StageStatus>();
        private readonly ExtendedCollection<AdditionalProgressCounter> _additionalProgressCounters = new ExtendedCollection<AdditionalProgressCounter>();

        internal const string DatePattern = "yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fff'Z'";

        // Used to support loading of old-style job status which didn't record this on a per-task basis.
        private int _rackLocalTaskCount = -1;
        private int _nonDataLocalTaskCount = -1;

        /// <summary>
        /// Gets or sets the ID of the job whose status this object represents.
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the display name of the job.
        /// </summary>
        public string JobName { get; set; }

        /// <summary>
        /// Gets the stages of this job.
        /// </summary>
        public Collection<StageStatus> Stages
        {
            get { return _stages; }
        }

        /// <summary>
        /// Gets the task attempts that failed.
        /// </summary>
        public Collection<TaskStatus> FailedTaskAttempts
        {
            get { return _failedTaskAttempts; }
        }

        /// <summary>
        /// Gets the additional progress counters.
        /// </summary>
        /// <value>The additional progress counters.</value>
        public Collection<AdditionalProgressCounter> AdditionalProgressCounters
        {
            get { return _additionalProgressCounters; }
        }

        /// <summary>
        /// Gets the total number of tasks in the job.
        /// </summary>
        public int TaskCount
        {
            get { return Stages.Sum(s => s.Tasks.Count); }
        }

        /// <summary>
        /// Gets or sets the number of tasks currently running.
        /// </summary>
        public int RunningTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that has not yet been scheduled.
        /// </summary>
        public int UnscheduledTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks that have finished.
        /// </summary>
        /// <remarks>
        /// This includes tasks that encountered an error.
        /// </remarks>
        public int FinishedTaskCount { get; set; }

        /// <summary>
        /// Gets or sets the number of data input tasks that were scheduled on the same rack as their input data, but not the same server.
        /// </summary>
        /// <value>The rack local task count.</value>
        public int RackLocalTaskCount
        {
            get { return _rackLocalTaskCount >= 0 ? _rackLocalTaskCount : GetTasksWithDistance(1); }
        }

        /// <summary>
        /// Gets or sets the number of data input tasks that were not scheduled on the same server or rack as their input data.
        /// </summary>
        /// <value>
        /// The non data local task count.
        /// </value>
        public int NonDataLocalTaskCount
        {
            get { return _nonDataLocalTaskCount >= 0 ? _nonDataLocalTaskCount : GetTasksWithDistance(2); }
        }

        /// <summary>
        /// Gets or sets the UTC start time of the 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the 
        /// </summary>
        /// <remarks>
        /// This property is not valid until the job is finished.
        /// </remarks>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the job has finished.
        /// </summary>
        public bool IsFinished { get; set; }

        /// <summary>
        /// Gets or sets the reason the job failed, if it failed.
        /// </summary>
        /// <value>The failure reason, or <see langword="null"/> if the job hasn't failed.</value>
        public string FailureReason { get; set; }

        /// <summary>
        /// Gets the number of task attempts that failed.
        /// </summary>
        [XmlIgnore]
        public int ErrorTaskCount
        {
            get
            {
                return FailedTaskAttempts == null ? 0 : FailedTaskAttempts.Count;
            }
        }

        /// <summary>
        /// Gets the total progress of the job, between 0 and 1.
        /// </summary>
        [XmlIgnore]
        public float Progress
        {
            get
            {
                return (from stage in Stages
                        from task in stage.Tasks
                        select task.Progress).Average();
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the job has finished successfully.
        /// </summary>
        public bool IsSuccessful
        {
            get { return FinishedTaskCount == TaskCount; }
        }

        /// <summary>
        /// Gets a string representation of this <see cref="JobStatus"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="JobStatus"/>.</returns>
        public override string ToString()
        {
            StringBuilder result = new StringBuilder(100);
            result.AppendFormat("{0:P1}; finished: {1}/{2} tasks", Progress, FinishedTaskCount, TaskCount);
            foreach( StageStatus stage in Stages )
            {
                result.AppendFormat("; {0}: {1:P1}", stage.StageId, stage.Progress);
            }
            if( ErrorTaskCount > 0 )
                result.AppendFormat(" ({0} errors)", ErrorTaskCount);

            return result.ToString();
        }

        /// <summary>
        /// Gets the friendly name for an additional progress counter.
        /// </summary>
        /// <param name="sourceName">Name of the source of the counter.</param>
        /// <returns>The friendly name of the counter.</returns>
        public string GetFriendlyNameForAdditionalProgressCounter(string sourceName)
        {
            return (from counter in _additionalProgressCounters
                    where counter.TypeName == sourceName
                    select counter.DisplayName ?? counter.TypeName).Single();
        }

        /// <summary>
        /// Gets the stage with the specified ID.
        /// </summary>
        /// <param name="stageId">The stage ID.</param>
        /// <returns>The specified stage, or <see langword="null"/> if no stage with the specified ID exists.</returns>
        public StageStatus GetStage(string stageId)
        {
            return (from stage in Stages
                    where stage.StageId == stageId
                    select stage).SingleOrDefault();
        }

        /// <summary>
        /// Gets an XML document containing the job status.
        /// </summary>
        /// <returns>An <see cref="XDocument"/> containing the job status.</returns>
        public XDocument ToXml()
        {
            return new XDocument(new XDeclaration("1.0", "utf-8", null), new XProcessingInstruction("xml-stylesheet", "type=\"text/xsl\" href=\"summary.xslt\""),
                new XElement("Job",
                    new XAttribute("id", JobId.ToString()),
                    new XAttribute("name", JobName),
                    new XElement("JobInfo",
                        new XAttribute("startTime", StartTime.ToString(DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("endTime", EndTime.ToString(DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("duration", (EndTime - StartTime).TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("tasks", TaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("finishedTasks", FinishedTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new XAttribute("errors", ErrorTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        _rackLocalTaskCount < 0 ? null : new XAttribute("rackLocalTasks", RackLocalTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        _nonDataLocalTaskCount < 0 ? null : new XAttribute("nonDataLocalTasks", NonDataLocalTaskCount.ToString(System.Globalization.CultureInfo.InvariantCulture))),
                    new XElement("Tasks",
                        from stage in Stages
                        from task in stage.Tasks
                        select task.ToXml()),
                    ErrorTaskCount == 0 ? 
                        null : 
                        new XElement("FailedTaskAttempts",
                            from task in FailedTaskAttempts
                            select task.ToXml()),
                    new XElement("StageMetrics",
                        from stage in Stages
                        select new XElement("Stage",
                            new XAttribute("id", stage.StageId),
                            stage.Metrics.ToXml()))
                )
            );
        }

        /// <summary>
        /// Creates a <see cref="JobStatus"/> instance from an XML element.
        /// </summary>
        /// <param name="job">The XML element containing the job status.</param>
        /// <returns>A new instance of the <see cref="JobStatus"/> class with the information from the XML document.</returns>
        public static JobStatus FromXml(XElement job)
        {
            if( job == null )
                throw new ArgumentNullException("job");
            if( job.Name != "Job" )
                throw new ArgumentException("Invalid job element.", "job");

            XElement jobInfo = job.Element("JobInfo");
            JobStatus jobStatus = new JobStatus()
            {
                JobId = new Guid(job.Attribute("id").Value),
                JobName = job.Attribute("name") == null ? null : job.Attribute("name").Value,
                StartTime = DateTime.ParseExact(jobInfo.Attribute("startTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                EndTime = DateTime.ParseExact(jobInfo.Attribute("endTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                FinishedTaskCount = (int)jobInfo.Attribute("finishedTasks"),
                _rackLocalTaskCount = jobInfo.Attribute("rackLocalTasks") == null ? -1 : (int)jobInfo.Attribute("rackLocalTasks"),
                _nonDataLocalTaskCount = jobInfo.Attribute("nonDataLocalTasks") == null ? -1 : (int)jobInfo.Attribute("nonDataLocalTasks"),
                IsFinished = true
            };

            var stages = from task in job.Element("Tasks").Elements("Task")
                         let taskStatus = TaskStatus.FromXml(task, jobStatus)
                         let taskId = new TaskId(taskStatus.TaskId)
                         group taskStatus by taskId.StageId;

            foreach( var stage in stages )
            {
                StageStatus stageStatus = new StageStatus() { StageId = stage.Key };
                stageStatus.Tasks.AddRange(stage);
                jobStatus.Stages.Add(stageStatus);
            }

            if( job.Element("FailedTaskAttempts") != null )
            {
                jobStatus.FailedTaskAttempts.AddRange(from task in job.Element("FailedTaskAttempts").Elements("Task")
                                                      select TaskStatus.FromXml(task, jobStatus));
            }

            XElement metricsElement = job.Element("StageMetrics");
            if( metricsElement != null )
            {
                foreach( XElement stage in metricsElement.Elements("Stage") )
                {
                    string stageId = stage.Attribute("id").Value;
                    jobStatus.GetStage(stageId).Metrics = TaskMetrics.FromXml(stage.Element("Metrics"));
                }
            }

            return jobStatus;

        }

        private int GetTasksWithDistance(int distance)
        {
            return (from stage in _stages
                    from task in stage.Tasks
                    where task.DataDistance == distance
                    select task).Count();
        }
    }
}
