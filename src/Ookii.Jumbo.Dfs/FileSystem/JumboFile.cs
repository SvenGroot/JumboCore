// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs.FileSystem;

/// <summary>
/// Provides information about a file on a file system accessible via the <see cref="FileSystemClient"/> class.
/// </summary>
[ValueWriter(typeof(Writer))]
public sealed class JumboFile : JumboFileSystemEntry
{
    #region Nested types

    /// <summary>
    /// The value writer for <see cref="JumboDirectory"/>.
    /// </summary>
    public class Writer : IValueWriter<JumboFile>
    {
        /// <inheritdoc/>
        public JumboFile Read(BinaryReader reader) => new(reader);

        /// <inheritdoc/>
        public void Write(JumboFile value, BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(nameof(value));
            ArgumentNullException.ThrowIfNull(nameof(writer));
            value.Serialize(writer);
            writer.Write(value._size);
            writer.Write(value._blockSize);
            writer.Write(value._replicationFactor);
            ValueWriter.WriteValue(value._recordOptions, writer);
            writer.Write(value._isOpenForWriting);
            ValueWriter.WriteValue(value.Blocks, writer);
        }
    }

    #endregion

    private static readonly ImmutableArray<Guid> _emptyBlocks = ImmutableArray.Create(Guid.Empty);

    private readonly long _size;
    private readonly long _blockSize;
    private readonly int _replicationFactor;
    private readonly RecordStreamOptions _recordOptions;
    private readonly bool _isOpenForWriting;

    /// <summary>
    /// Initializes a new instance of the <see cref="JumboFile"/> class.
    /// </summary>
    /// <param name="fullPath">The full path of the file.</param>
    /// <param name="name">The name of the file.</param>
    /// <param name="dateCreated">The date and time the file was created.</param>
    /// <param name="size">The size of the file.</param>
    /// <param name="blockSize">The block size of the file.</param>
    /// <param name="replicationFactor">The number of replicas.</param>
    /// <param name="recordOptions">The record options.</param>
    /// <param name="isOpenForWriting">if set to <see langword="true"/> the file is open for writing.</param>
    /// <param name="blocks">The blocks that make up this file. May be <see langword="null"/>.</param>
    public JumboFile(string fullPath, string name, DateTime dateCreated, long size, long blockSize, int replicationFactor, RecordStreamOptions recordOptions, bool isOpenForWriting, IEnumerable<Guid>? blocks)
        : base(fullPath, name, dateCreated)
    {
        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        if (blockSize < 0)
        {
            if (replicationFactor < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(replicationFactor));
            }
        }

        _size = size;
        _blockSize = blockSize;
        _replicationFactor = replicationFactor;
        _recordOptions = recordOptions;
        _isOpenForWriting = isOpenForWriting;
        Blocks = blocks?.ToImmutableArray() ?? _emptyBlocks;
    }

    private JumboFile(BinaryReader reader)
        : base(reader)
    {
        _size = reader.ReadInt64();
        _blockSize = reader.ReadInt64();
        _replicationFactor = reader.ReadInt32();
        _recordOptions = ValueWriter<RecordStreamOptions>.ReadValue(reader);
        _isOpenForWriting = reader.ReadBoolean();
        Blocks = ValueWriter<ImmutableArray<Guid>>.ReadValue(reader);
    }

    /// <summary>
    /// Gets the size of the file.
    /// </summary>
    /// <value>
    /// The size of the file.
    /// </value>
    public long Size
    {
        get { return _size; }
    }

    /// <summary>
    /// Gets the size of the blocks that the file was divided into.
    /// </summary>
    /// <value>
    /// The size of the blocks, or the size of the file if the file system doesn't support blocks.
    /// </value>
    /// <remarks>
    /// </remarks>
    public long BlockSize
    {
        get { return _blockSize; }
    }

    /// <summary>
    /// Gets the number of replicas of this file.
    /// </summary>
    /// <value>
    /// The number of replicas of this file. If the file system doesn't support replication, this value will be 1.
    /// </value>
    public int ReplicationFactor
    {
        get { return _replicationFactor; }
    }

    /// <summary>
    /// Gets the record options applied to this file.
    /// </summary>
    /// <value>
    /// The record options. If the file system doesn't support record options, this value will be <see langword="RecordStreamOptions.None"/>
    /// </value>
    public RecordStreamOptions RecordOptions
    {
        get { return _recordOptions; }
    }

    /// <summary>
    /// Gets a value indicating whether this file is open for writing.
    /// </summary>
    /// <value>
    /// 	<see langword="true"/> if this file is open for writing; otherwise, <see langword="false"/>. If the
    /// 	file system doesn't support reporting this information, the value will always be <see langword="false"/>.
    /// </value>
    public bool IsOpenForWriting
    {
        get { return _isOpenForWriting; }
    }

    /// <summary>
    /// Gets the IDs of the blocks of this file.
    /// </summary>
    /// <value>
    /// A list of block IDs, or a list containing <see cref="Guid.Empty"/> if this file system doesn't support blocks.
    /// </value>
    public ImmutableArray<Guid> Blocks { get; }

    /// <summary>
    /// Creates a <see cref="JumboFile"/> instance for a local file from the specified <see cref="FileInfo"/>.
    /// </summary>
    /// <param name="file">The <see cref="FileInfo"/>.</param>
    /// <param name="rootPath">The root path of the file system.</param>
    /// <returns>
    /// A <see cref="JumboFile"/> instance for the local file.
    /// </returns>
    public static JumboFile FromFileInfo(FileInfo file, string? rootPath)
    {
        ArgumentNullException.ThrowIfNull(file);
        if (!file.Exists)
        {
            throw new FileNotFoundException(string.Format(CultureInfo.CurrentCulture, "The file '{0}' does not exist.", file.FullName), file.FullName);
        }

        return new JumboFile(StripRootPath(file.FullName, rootPath), file.Name, file.CreationTimeUtc, file.Length, file.Length, 1, RecordStreamOptions.None, false, null);
    }

    /// <summary>
    /// Gets a string representation of this file.
    /// </summary>
    /// <returns>A string representation of this file.</returns>
    public override string ToString()
    {
        return string.Format(System.Globalization.CultureInfo.InvariantCulture, ListingEntryFormat, DateCreated.ToLocalTime(), Size, Name);
    }

    /// <summary>
    /// Prints information about the file.
    /// </summary>
    /// <param name="writer">The <see cref="System.IO.TextWriter"/> to write the information to.</param>
    public void PrintFileInfo(System.IO.TextWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteLine("Path:             {0}", FullPath);
        writer.WriteLine("Size:             {0:#,0} bytes", Size);
        writer.WriteLine("Block size:       {0:#,0} bytes", BlockSize);
        writer.WriteLine("Replicas:         {0}", ReplicationFactor);
        writer.WriteLine("Record options:   {0}", RecordOptions);
        writer.WriteLine("Open for writing: {0}", IsOpenForWriting);
        writer.WriteLine("Blocks:           {0}", Blocks.Length);
        foreach (var block in Blocks)
        {
            writer.WriteLine("{{{0}}}", block);
        }
    }
}
