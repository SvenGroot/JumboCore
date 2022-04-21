// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet
{
    /// <summary>
    /// Represents the progress of a task.
    /// </summary>
    [Serializable]
    public class TaskProgress
    {
        private List<AdditionalProgressValue> _additionalProgressValues;

        /// <summary>
        /// Gets or sets the base progress value. This is the progress of the input reader.
        /// </summary>
        /// <value>The progress value.</value>
        public float Progress { get; set; }

        /// <summary>
        /// Gets or sets a status message for the task.
        /// </summary>
        public string StatusMessage { get; set; }

        /// <summary>
        /// Gets the overall progress, which is the average of all the other progress values.
        /// </summary>
        /// <value>The overall progress.</value>
        public float OverallProgress
        {
            get
            {
                if (_additionalProgressValues == null)
                    return Progress;
                else
                    return (Progress + _additionalProgressValues.Sum(x => x.Progress)) / (float)(_additionalProgressValues.Count + 1);
            }
        }

        /// <summary>
        /// Gets the additional progress values.
        /// </summary>
        /// <value>The additional progress values.</value>
        public ReadOnlyCollection<AdditionalProgressValue> AdditionalProgressValues
        {
            get { return _additionalProgressValues == null ? null : _additionalProgressValues.AsReadOnly(); }
        }

        /// <summary>
        /// Adds an additional progress value.
        /// </summary>
        /// <param name="typeName">Name of the type that is the source of the value.</param>
        /// <param name="value">The progress value.</param>
        public void AddAdditionalProgressValue(string typeName, float value)
        {
            if (_additionalProgressValues == null)
                _additionalProgressValues = new List<AdditionalProgressValue>();
            _additionalProgressValues.Add(new AdditionalProgressValue() { SourceName = typeName, Progress = value });
        }

        /// <summary>
        /// Sets all progress values to 100%.
        /// </summary>
        public void SetFinished()
        {
            Progress = 1.0f;
            if (_additionalProgressValues != null)
            {
                foreach (AdditionalProgressValue value in _additionalProgressValues)
                    value.Progress = 1.0f;
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            if (_additionalProgressValues == null)
                return Progress.ToString("P1", CultureInfo.InvariantCulture);
            else
                return string.Format(CultureInfo.InvariantCulture, "Overall: {0:P1}; Base: {1:P1}; {2}", OverallProgress, Progress, _additionalProgressValues.ToDelimitedString("; "));
        }
    }
}
