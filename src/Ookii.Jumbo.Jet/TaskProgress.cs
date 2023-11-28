// Copyright (c) Sven Groot (Ookii.org)
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Represents the progress of a task.
/// </summary>
[GeneratedWritable]
public partial class TaskProgress
{
    /// <summary>
    /// Gets or sets the base progress value. This is the progress of the input reader.
    /// </summary>
    /// <value>The progress value.</value>
    public float Progress { get; set; }

    /// <summary>
    /// Gets or sets a status message for the task.
    /// </summary>
    public string? StatusMessage { get; set; }

    /// <summary>
    /// Gets the overall progress, which is the average of all the other progress values.
    /// </summary>
    /// <value>The overall progress.</value>
    public float OverallProgress
    {
        get
        {
            if (AdditionalProgressValuesList == null)
            {
                return Progress;
            }
            else
            {
                return (Progress + AdditionalProgressValuesList.Sum(x => x.Progress)) / (AdditionalProgressValuesList.Count + 1);
            }
        }
    }

    /// <summary>
    /// Gets the additional progress values.
    /// </summary>
    /// <value>The additional progress values.</value>
    [WritableIgnore]
    public ReadOnlyCollection<AdditionalProgressValue>? AdditionalProgressValues => AdditionalProgressValuesList?.AsReadOnly();

    // This is a property so it gets serialized by the generated IWritable implementation.
    private List<AdditionalProgressValue>? AdditionalProgressValuesList { get; set; }

    /// <summary>
    /// Adds an additional progress value.
    /// </summary>
    /// <param name="typeName">Name of the type that is the source of the value.</param>
    /// <param name="value">The progress value.</param>
    public void AddAdditionalProgressValue(string? typeName, float value)
    {
        AdditionalProgressValuesList ??= new List<AdditionalProgressValue>();
        AdditionalProgressValuesList.Add(new AdditionalProgressValue() { SourceName = typeName, Progress = value });
    }

    /// <summary>
    /// Sets all progress values to 100%.
    /// </summary>
    public void SetFinished()
    {
        Progress = 1.0f;
        if (AdditionalProgressValuesList != null)
        {
            foreach (var value in AdditionalProgressValuesList)
            {
                value.Progress = 1.0f;
            }
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
        if (AdditionalProgressValuesList == null)
        {
            return Progress.ToString("P1", CultureInfo.InvariantCulture);
        }
        else
        {
            return string.Format(CultureInfo.InvariantCulture, "Overall: {0:P1}; Base: {1:P1}; {2}", OverallProgress, Progress, AdditionalProgressValuesList.ToDelimitedString("; "));
        }
    }
}
