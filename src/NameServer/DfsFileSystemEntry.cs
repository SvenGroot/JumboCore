// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Ookii.Jumbo.Dfs;
using Ookii.Jumbo.Dfs.FileSystem;

namespace NameServerApplication;

/// <summary>
/// Represents a file or directory in the distributed file system namespace.
/// </summary>
abstract class DfsFileSystemEntry
{
    /// <summary>
    /// The format string for printing entries in a directory listing.
    /// </summary>
    /// <remarks>
    /// The parameters should be, in order: creation date, size (or a string saying &lt;DIR&gt; for
    /// directories), name.
    /// </remarks>
    protected const string ListingEntryFormat = "{0:yyyy-MM-dd HH:mm}  {1,15:#,0}  {2}";

    /// <summary>
    /// Initializes a new instance of the <see cref="DfsFileSystemEntry"/> class.
    /// </summary>
    /// <param name="parent">The parent of the entry. May be <see langword="null" />.</param>
    /// <param name="name">The name of the new entry.</param>
    /// <param name="dateCreated">The date the new entry was created.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException"><paramref name="name"/> contains the / character.</exception>
    protected DfsFileSystemEntry(DfsDirectory? parent, string name, DateTime dateCreated)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (name.Contains(DfsPath.DirectorySeparator, StringComparison.Ordinal))
        {
            throw new ArgumentException("File or directory name cannot contain directory separator.", nameof(name));
        }

        Name = name;
        DateCreated = dateCreated;

        if (parent != null)
        {
            parent.Children.Add(this);
            Parent = parent;
        }
    }

    /// <summary>
    /// Gets or sets the name of the file system entry.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Gets the date and time the file system entry was created.
    /// </summary>
    public DateTime DateCreated { get; private set; }

    /// <summary>
    /// Gets the parent directory of the file system entry. This will be <see langword="null"/> on objects created by <see cref="ShallowClone" />.
    /// </summary>
    private DfsDirectory? Parent { get; set; }

    /// <summary>
    /// Gets the absolute path of the file system entry.
    /// </summary>
    public string FullPath
    {
        get
        {
            if (Parent == null)
            {
                return DfsPath.DirectorySeparator.ToString(CultureInfo.InvariantCulture);
            }
            else
            {
                var path = new StringBuilder();
                BuildPath(path);
                return path.ToString();
            }
        }
    }

    /// <summary>
    /// Moves the entry to a new parent.
    /// </summary>
    /// <param name="newParent">The new parent of the entry.</param>
    /// <param name="newName">The new name of the entry. Can be <see langword="null"/>.</param>
    public void MoveTo(DfsDirectory newParent, string? newName)
    {
        ArgumentNullException.ThrowIfNull(newParent);

        if (Parent == null)
        {
            throw new InvalidOperationException("You cannot move an entry without an existing parent.");
        }

        if (newParent != Parent || newName != null)
        {
            var name = newName ?? Name;
            if ((from child in newParent.Children where child.Name == name select child).Count() > 0)
            {
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "The specified new parent already contains an entry with the name \"{0}\".", newName));
            }
        }

        if (newName != null)
        {
            Name = newName;
        }

        if (newParent != Parent)
        {
            Parent.Children.Remove(this);
            newParent.Children.Add(this);
            Parent = newParent;
        }
    }

    /// <summary>
    /// Saves this <see cref="DfsFileSystemEntry"/> to a file system image.
    /// </summary>
    /// <param name="writer">A <see cref="BinaryWriter"/> used to write to the file system image.</param>
    public virtual void SaveToFileSystemImage(BinaryWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.Write(GetType().FullName!);
        writer.Write(Name);
        writer.Write(DateCreated.Ticks);
    }

    /// <summary>
    /// Loads a <see cref="DfsFileSystemEntry"/> from the file system image.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> used to read the file system image.</param>
    /// <param name="parent">The parent directory of the new <see cref="DfsFileSystemEntry"/>.</param>
    /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
    /// <returns>An instance of <see cref="DfsFile"/> or <see cref="DfsDirectory"/> representing the file system entry.</returns>
    public static DfsFileSystemEntry LoadFromFileSystemImage(BinaryReader reader, DfsDirectory? parent, Action<long> notifyFileSizeCallback)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var className = reader.ReadString();
        var name = reader.ReadString();
        var dateCreated = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
        DfsFileSystemEntry entry;
        if (className == typeof(DfsFile).FullName)
        {
            entry = new DfsFile(parent, name, dateCreated);
        }
        else if (className == typeof(DfsDirectory).FullName)
        {
            entry = new DfsDirectory(parent, name, dateCreated);
        }
        else
        {
            throw new DfsException("Invalid file system image.");
        }

        entry.LoadFromFileSystemImage(reader, notifyFileSizeCallback);

        return entry;
    }

    /// <summary>
    /// Creates a <see cref="JumboFileSystemEntry"/> from this <see cref="DfsFileSystemEntry"/>.
    /// </summary>
    /// <param name="includeChildren">if set to <see langword="true"/> include the children if this is a directory.</param>
    /// <returns>
    /// A <see cref="JumboFileSystemEntry"/>.
    /// </returns>
    public abstract JumboFileSystemEntry ToJumboFileSystemEntry(bool includeChildren = true);

    /// <summary>
    /// When implemented in a derived class, reads information about the <see cref="DfsFileSystemEntry"/> from the file system image.
    /// </summary>
    /// <param name="reader">The <see cref="BinaryReader"/> used to read the file system image.</param>
    /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
    protected abstract void LoadFromFileSystemImage(BinaryReader reader, Action<long> notifyFileSizeCallback);

    private void BuildPath(StringBuilder path)
    {
        if (Parent != null)
        {
            Parent.BuildPath(path);
            path.Append(DfsPath.DirectorySeparator);
            path.Append(Name);
        }
    }
}
