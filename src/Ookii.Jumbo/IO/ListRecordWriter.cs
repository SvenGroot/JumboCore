// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Ookii.Jumbo.IO;

/// <summary>
/// Record writer that writes the items to a list.
/// </summary>
/// <typeparam name="T">The type of record.</typeparam>
public class ListRecordWriter<T> : RecordWriter<T>
    where T : notnull
{
    private readonly List<T> _list = new List<T>();
    private readonly bool _cloneRecords;

    /// <summary>
    /// Initializes a new instance of the <see cref="ListRecordWriter&lt;T&gt;"/> class.
    /// </summary>
    public ListRecordWriter()
        : this(false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ListRecordWriter&lt;T&gt;"/> class.
    /// </summary>
    /// <param name="cloneRecords"><see langword="true"/> to clone records before adding them to the list; otherwise, <see langword="false"/>.</param>
    /// <remarks>
    /// <para>
    ///   If <paramref name="cloneRecords"/> is <see langword="true"/>, the type <typeparamref name="T"/> must implement <see cref="ICloneable"/>.
    /// </para>
    /// </remarks>
    public ListRecordWriter(bool cloneRecords)
    {
        if (cloneRecords && !typeof(T).GetInterfaces().Contains(typeof(ICloneable)))
        {
            throw new ArgumentException("If cloneRecords is true, the type T must implement ICloneable.");
        }

        _cloneRecords = cloneRecords;
    }

    /// <summary>
    /// Gets the list to which the records are written.
    /// </summary>
    public ReadOnlyCollection<T> List
    {
        get { return _list.AsReadOnly(); }
    }

    /// <summary>
    /// Writes a record.
    /// </summary>
    /// <param name="record">The record to write.</param>
    protected override void WriteRecordInternal(T record)
    {
        if (_cloneRecords)
        {
            _list.Add((T)((ICloneable)record).Clone());
        }
        else
        {
            _list.Add(record);
        }
    }
}
