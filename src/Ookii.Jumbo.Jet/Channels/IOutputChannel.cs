﻿// Copyright (c) Sven Groot (Ookii.org)
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Channels;

/// <summary>
/// Interface for output channels for task communication.
/// </summary>
public interface IOutputChannel
{
    /// <summary>
    /// Creates a <see cref="RecordWriter{T}"/> to which the channel can write its output.
    /// </summary>
    /// <typeparam name="T">The type of the records.</typeparam>
    /// <returns>A <see cref="RecordWriter{T}"/> for the channel.</returns>
    RecordWriter<T> CreateRecordWriter<T>()
        where T : notnull;
}
