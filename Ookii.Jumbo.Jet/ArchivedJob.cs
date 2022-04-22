// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Represents information about a job that has been archived.
    /// </summary>
    [Serializable]
    public sealed class ArchivedJob : IWritable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ArchivedJob"/> class.
        /// </summary>
        public ArchivedJob()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArchivedJob"/> class.
        /// </summary>
        /// <param name="job">The job.</param>
        public ArchivedJob(JobStatus job)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            JobId = job.JobId;
            JobName = job.JobName;
            IsSuccessful = job.IsSuccessful;
            StartTime = job.StartTime;
            EndTime = job.EndTime;
            TaskCount = job.TaskCount;
        }

        /// <summary>
        /// Gets or sets the ID of the job.
        /// </summary>
        /// <value>The job ID.</value>
        public Guid JobId { get; set; }

        /// <summary>
        /// Gets or sets the friendly name of the job.
        /// </summary>
        /// <value>The friendly name of the job.</value>
        public string JobName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this job succeeded
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if this job succeeded; otherwise, <see langword="false"/>.
        /// </value>
        public bool IsSuccessful { get; set; }

        /// <summary>
        /// Gets or sets the UTC start time of the job.
        /// </summary>
        /// <value>The start time in UTC.</value>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets the UTC end time of the job.
        /// </summary>
        /// <value>The end time in UTC.</value>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Gets or sets the number of tasks in the job.
        /// </summary>
        /// <value>The number of tasks in the job.</value>
        public int TaskCount { get; set; }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            writer.Write(JobId.ToByteArray());
            writer.Write(JobName != null);
            if (JobName != null)
                writer.Write(JobName);
            writer.Write(IsSuccessful);
            writer.Write(StartTime.Ticks);
            writer.Write(EndTime.Ticks);
            writer.Write(TaskCount);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            JobId = new Guid(reader.ReadBytes(16));
            if (reader.ReadBoolean())
                JobName = reader.ReadString();
            IsSuccessful = reader.ReadBoolean();
            StartTime = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
            EndTime = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
            TaskCount = reader.ReadInt32();
        }
    }
}
