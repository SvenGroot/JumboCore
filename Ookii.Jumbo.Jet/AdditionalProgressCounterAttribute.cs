// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Indicates that a task, multi record reader or channel reports additional progress.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class AdditionalProgressCounterAttribute : Attribute
    {
        private readonly string _displayName;

        /// <summary>
        /// Initializes a new instance of the <see cref="AdditionalProgressCounterAttribute"/> class.
        /// </summary>
        /// <param name="displayName">The id of the progress counter.</param>
        public AdditionalProgressCounterAttribute(string displayName)
        {
            if (displayName == null)
                throw new ArgumentNullException(nameof(displayName));

            _displayName = displayName;
        }

        /// <summary>
        /// Gets the name of the counter.
        /// </summary>
        /// <value>The name of the counter.</value>
        public string DisplayName
        {
            get { return _displayName; }
        }
    }
}
