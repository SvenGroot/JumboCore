namespace Ookii.Jumbo.IO;

/// <summary>
/// Interface for types implementing the <see cref="IRawComparer{T}"/> interface that may use deserialization;
/// </summary>
public interface IDeserializingRawComparer
{
    /// <summary>
    /// Gets a value indicating whether the comparer uses deserialization.
    /// </summary>
    /// <value>
    /// <see langword="true" /> if whether the comparer uses deserialization; otherwise, <see langword="false" />.
    /// </value>
    bool UsesDeserialization { get; }
}
