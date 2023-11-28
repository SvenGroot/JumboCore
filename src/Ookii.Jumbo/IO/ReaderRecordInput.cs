// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO;

/// <summary>
/// An implementation of <see cref="RecordInput"/> that reads the input from an existing record reader.
/// </summary>
public sealed class ReaderRecordInput : RecordInput
{
    private readonly bool _isMemoryBased;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReaderRecordInput"/> class.
    /// </summary>
    /// <param name="reader">The record reader for this input.</param>
    /// <param name="isMemoryBased">If set to <see langword="true"/>, the input is memory based; if <see langword="false" />, the input is read from a file.</param>
    public ReaderRecordInput(IRecordReader reader, bool isMemoryBased)
        : base(reader)
    {
        _isMemoryBased = isMemoryBased;
    }

    /// <summary>
    /// Gets a value indicating whether this input is read from memory.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if this input is read from memory; <see langword="false"/> if it is read from a file.
    /// </value>
    public override bool IsMemoryBased
    {
        get { return _isMemoryBased; }
    }

    /// <summary>
    /// Gets a value indicating whether this instance supports the raw record reader.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if this instance supports the raw record reader; otherwise, <see langword="false"/>.
    /// </value>
    public override bool IsRawReaderSupported
    {
        get { return false; }
    }

    /// <summary>
    /// Creates the record reader for this input. This function is not used by the <see cref="ReaderRecordInput"/> class.
    /// </summary>
    /// <returns>
    /// The record reader for this input.
    /// </returns>
    protected override IRecordReader CreateReader()
    {
        // Not called if the RecordInput.RecordInput(IRecordReader) constructor is used.
        throw new NotSupportedException();
    }

    /// <summary>
    /// Creates the raw record reader for this input. This function is not supported by the <see cref="ReaderRecordInput"/> class.
    /// </summary>
    /// <returns>
    /// The record reader for this input.
    /// </returns>
    protected override RecordReader<RawRecord> CreateRawReader()
    {
        throw new NotSupportedException("This input doesn't support raw record readers.");
    }
}
