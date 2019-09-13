// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Globalization;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Provides status information about a task.
    /// </summary>
    [Serializable]
    public class TaskStatus
    {
        /// <summary>
        /// Gets or sets the ID of this task.
        /// </summary>
        public string TaskId { get; set; }

        /// <summary>
        /// Gets or sets the current state of the task.
        /// </summary>
        public TaskState State { get; set; }

        /// <summary>
        /// Gets or sets the task server that a job is assigned to.
        /// </summary>
        /// <remarks>
        /// If there has been more than one attempt, this information only applies to the current attempt.
        /// </remarks>
        public ServerAddress TaskServer { get; set; }

        /// <summary>
        /// Gets or sets the number of times this task has been attempted.
        /// </summary>
        public int Attempts { get; set; }

        /// <summary>
        /// Gets or sets the UTC start time of the task.
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the task.
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets the progress of the task.
        /// </summary>
        public TaskProgress TaskProgress { get; set; }

        /// <summary>
        /// Gets or sets the metrics collected during task execution.
        /// </summary>
        public TaskMetrics Metrics { get; set; }

        /// <summary>
        /// Gets the overall progress of the task.
        /// </summary>
        /// <value>The overall progress.</value>
        public float Progress
        {
            get { return TaskProgress == null ? 0.0f : TaskProgress.OverallProgress; }
        }

        /// <summary>
        /// Gets or sets the distance to the input data, if this task read from the DFS.
        /// </summary>
        /// <value>-1 if this task didn't read from the DFS, 0 if this task was scheduled data-local, 1 if it was rack-local, 2 if it was neither data nor rack-local.</value>
        public int DataDistance { get; set; }

        /// <summary>
        /// Gets the duration of the task.
        /// </summary>
        public TimeSpan Duration
        {
            get
            {
                return EndTime - StartTime;
            }
        }

        /// <summary>
        /// The amount of time after the start of the job that this task started.
        /// </summary>
        [XmlIgnore]
        public TimeSpan StartOffset { get; set; }

        /// <summary>
        /// Gets an XML element containing the task status.
        /// </summary>
        /// <returns>An <see cref="XElement"/> containing the task status.</returns>
        public XElement ToXml()
        {
            return new XElement("Task",
                new XAttribute("id", TaskId),
                new XAttribute("state", State.ToString()),
                new XAttribute("server", TaskServer == null ? "" : TaskServer.ToString()),
                new XAttribute("attempts", Attempts.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("startTime", StartTime.ToString(JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("endTime", EndTime.ToString(JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture)),
                new XAttribute("duration", Duration.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                DataDistance < 0 ? null : new XAttribute("dataDistance", DataDistance.ToString(CultureInfo.InvariantCulture)));
        }

        /// <summary>
        /// Creates a <see cref="TaskStatus"/> instance from an XML element.
        /// </summary>
        /// <param name="task">The XML element containing the task status.</param>
        /// <param name="job">The job that this task belongs to.</param>
        /// <returns>A new instance of the <see cref="TaskStatus"/> class with the information from the XML document.</returns>
        public static TaskStatus FromXml(XElement task, JobStatus job)
        {
            if( task == null )
                throw new ArgumentNullException("task");
            if( job == null )
                throw new ArgumentNullException("job");

            if( task.Name != "Task" )
                throw new ArgumentException("Invalid task element.", "task");

            TaskStatus status = new TaskStatus()
            {
                TaskId = task.Attribute("id").Value,
                State = (TaskState)Enum.Parse(typeof(TaskState), task.Attribute("state").Value),
                TaskServer = string.IsNullOrEmpty(task.Attribute("server").Value) ? null : new ServerAddress(task.Attribute("server").Value),
                Attempts = (int)task.Attribute("attempts"),
                StartTime = DateTime.ParseExact(task.Attribute("startTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                EndTime = DateTime.ParseExact(task.Attribute("endTime").Value, JobStatus.DatePattern, System.Globalization.CultureInfo.InvariantCulture),
                TaskProgress = new TaskProgress() { Progress = 1f },
                DataDistance = task.Attribute("dataDistance") == null ? -1 : (int)task.Attribute("dataDistance")
            };
            status.StartOffset = status.StartTime - job.StartTime;
            return status;
        }
    }
}
