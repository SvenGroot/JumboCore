// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo.IO;

/// <summary>
/// Interface for streams that offer special handling of records.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix", Justification = "Interface should only be applied to streams.")]
public interface IRecordOutputStream
{
    /// <summary>
    /// Gets the options applied to records in the stream.
    /// </summary>
    /// <value>One or more of the <see cref="RecordStreamOptions"/> values.</value>
    RecordStreamOptions RecordOptions { get; }

    /// <summary>
    /// Gets the amount of the stream that is actually used by records.
    /// </summary>
    /// <value>The length of the stream minus padding.</value>
    long RecordsSize { get; }

    /// <summary>
    /// Indicates that the current position of the stream is a record boundary.
    /// </summary>
    void MarkRecord();
}
