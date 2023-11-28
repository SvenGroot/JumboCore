// Copyright (c) Sven Groot (Ookii.org)
using System;
using Ookii.Jumbo.Jet.Jobs;

namespace Ookii.Jumbo.Jet.IO;

/// <summary>
/// Provides method for defining data output (other than a channel) for a stage.
/// </summary>
public interface IDataOutput
{
    /// <summary>
    /// Gets the type of the records used for this output.
    /// </summary>
    /// <value>
    /// The type of the records.
    /// </value>
    Type RecordType { get; }

    /// <summary>
    /// Creates the output for the specified partition.
    /// </summary>
    /// <param name="partitionNumber">The partition number for this output.</param>
    /// <returns>
    /// The record writer.
    /// </returns>
    IOutputCommitter CreateOutput(int partitionNumber);

    /// <summary>
    /// Notifies the data input that it has been added to a stage.
    /// </summary>
    /// <param name="stage">The stage configuration of the stage.</param>
    /// <remarks>
    /// <para>
    ///   Implement this method if you want to add any setting to the stage. Keep in mind that the stage may still be under construction, so not all its
    ///   properties may have their final values yet.
    /// </para>
    /// </remarks>
    void NotifyAddedToStage(StageConfiguration stage);
}
