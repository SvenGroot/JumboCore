using System;
using System.Collections.Generic;
using System.Linq;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet;

/// <summary>
/// Contains the result of a merge operation.
/// </summary>
/// <typeparam name="T">The type of the record.</typeparam>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix")]
public class MergeResult<T> : IEnumerable<MergeResultRecord<T>>
    where T : notnull
{
    private readonly IRecordReader[]? _readers; // No need to dispose these; that'll be taken care of by the merger
    private IEnumerator<MergeResultRecord<T>>? _mergeResult;

    internal MergeResult(IRecordReader[]? readers, IEnumerator<MergeResultRecord<T>> result)
    {
        _readers = readers;
        _mergeResult = result;
    }

    /// <summary>
    /// Gets the progress of the merge pass.
    /// </summary>
    public float Progress
    {
        get { return _readers == null || _readers.Length == 0 ? 1.0f : _readers.Average(r => r.Progress); }
    }

    /// <summary>
    /// Gets the enumerator.
    /// </summary>
    /// <returns>The enumerator.</returns>
    public IEnumerator<MergeResultRecord<T>> GetEnumerator()
    {
        if (_mergeResult == null)
        {
            throw new InvalidOperationException("Merge pass can be enumerated only once.");
        }

        var result = _mergeResult;
        _mergeResult = null;
        return result;
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
