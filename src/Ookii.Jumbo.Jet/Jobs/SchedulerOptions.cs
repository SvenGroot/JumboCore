// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Xml.Serialization;

namespace Ookii.Jumbo.Jet.Jobs
{
    /// <summary>
    /// Provides job configuration options to configure the behaviour of the scheduler.
    /// </summary>
    [XmlType("SchedulerOptions", Namespace = JobConfiguration.XmlNamespace)]
    public sealed class SchedulerOptions
    {
        private int _maximumDataDistance = 2;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchedulerOptions"/> class.
        /// </summary>
        public SchedulerOptions()
        {
        }

        /// <summary>
        /// Gets or sets the maximum distance from the input data for a data input task.
        /// </summary>
        /// <value>Zero to allow only data-local tasks, one to also allow rack-local tasks, two or higher to also allow non-local tasks. The default value is two.</value>
        [XmlAttribute("maximumDataDistance")]
        public int MaximumDataDistance
        {
            get { return _maximumDataDistance; }
            set
            {
                if (_maximumDataDistance < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));
                _maximumDataDistance = value;
            }
        }


        /// <summary>
        /// Gets or sets a value indicating how the server will assign DFS input tasks to task servers.
        /// </summary>
        /// <value>
        /// 	One of the values of the <see cref="SchedulingMode"/> enumeration.
        /// </value>
        /// <remarks>
        /// <para>
        ///   When this property is set to <see cref="SchedulingMode.MoreServers"/>, the scheduler will prefer the server with the most available tasks, while
        ///   <see cref="SchedulingMode.FewerServers"/> means it will prefer the server with the fewest available tasks. Note that in either case, it will
        ///   still prefer a local task of a non-local one regardless of the number of available tasks.
        /// </para>
        /// <para>
        ///   When this property is set to <see cref="SchedulingMode.OptimalLocality"/>, the scheduler will attempt to schedule in a way that minimizes the
        ///   number of non-local tasks, without looking at the number of available tasks on the server.
        /// </para>
        /// </remarks>
        [XmlAttribute("dataInputSchedulingMode")]
        public SchedulingMode DataInputSchedulingMode { get; set; }

        /// <summary>
        /// Gets or sets a value indicating how the server will assign tasks that do not have data input to task servers.
        /// </summary>
        /// <value>
        /// 	One of the values of the <see cref="SchedulingMode"/> enumeration.
        /// </value>
        /// <remarks>
        /// <para>
        ///   When this property is set to <see cref="SchedulingMode.MoreServers"/>, the scheduler will prefer the server with the most available tasks, while
        ///   <see cref="SchedulingMode.FewerServers"/> means it will prefer the server with the fewest available tasks.
        /// </para>
        /// <para>
        ///   The value of <see cref="SchedulingMode.OptimalLocality"/> is not valid for this property; it will be treated as <see cref="SchedulingMode.Default"/>.
        /// </para>
        /// </remarks>
        [XmlAttribute("nonDataInputSchedulingMode")]
        public SchedulingMode NonDataInputSchedulingMode { get; set; }
    }
}
