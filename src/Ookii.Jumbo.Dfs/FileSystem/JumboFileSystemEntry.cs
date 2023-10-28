// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Globalization;
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides information about a file accessible from a <see cref="JumboFileSystemEntry"/>
    /// </summary>
    [ValueWriter(typeof(PolymorphicValueWriter<JumboFileSystemEntry>))]
    [WritableDerivedType(typeof(JumboFile))]
    [WritableDerivedType(typeof(JumboDirectory))]
    public abstract class JumboFileSystemEntry
    {
        private readonly string _fullPath;
        private readonly string _name;
        private readonly DateTime _dateCreated;

        /// <summary>
        /// The format string for printing entries in a directory listing.
        /// </summary>
        /// <remarks>
        /// The parameters should be, in order: creation date, size (or a string saying &lt;DIR&gt; for
        /// directories), name.
        /// </remarks>
        protected const string ListingEntryFormat = "{0:yyyy-MM-dd HH:mm}  {1,15:#,0}  {2}";

        /// <summary>
        /// Initializes a new instance of the <see cref="JumboFileSystemEntry"/> class.
        /// </summary>
        /// <param name="fullPath">The full path.</param>
        /// <param name="name">The name.</param>
        /// <param name="dateCreated">The date the entry was created.</param>
        protected JumboFileSystemEntry(string fullPath, string name, DateTime dateCreated)
        {
            ArgumentNullException.ThrowIfNull(fullPath);
            ArgumentNullException.ThrowIfNull(name);

            _fullPath = fullPath;
            _name = name;
            _dateCreated = dateCreated;
        }

        internal JumboFileSystemEntry(BinaryReader reader)
        {
            _fullPath = reader.ReadString();
            _name = reader.ReadString();
            _dateCreated = ValueWriter<DateTime>.ReadValue(reader);
        }

        /// <summary>
        /// Gets the absolute path to the file system entry.
        /// </summary>
        /// <value>
        /// The absolute path to the file system entry.
        /// </value>
        public string FullPath
        {
            get { return _fullPath; }
        }

        /// <summary>
        /// Gets the name of the file system entry.
        /// </summary>
        /// <value>
        /// The name of the file system entry.
        /// </value>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets the date and time the file system entry was created.
        /// </summary>
        /// <value>
        /// The date and time the file system entry was created.
        /// </value>
        public DateTime DateCreated
        {
            get { return _dateCreated; }
        }

        /// <summary>
        /// Creates a <see cref="JumboFileSystemEntry"/> instance for a local file or directory from a <see cref="FileSystemInfo"/>.
        /// </summary>
        /// <param name="info">The <see cref="FileSystemInfo"/>.</param>
        /// <param name="rootPath">The root path of the file system.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/> instance for the local file or directory.
        /// </returns>
        public static JumboFileSystemEntry FromFileSystemInfo(FileSystemInfo info, string rootPath)
        {
            return FromFileSystemInfo(info, rootPath, true);
        }

        /// <summary>
        /// Creates a <see cref="JumboFileSystemEntry"/> instance for a local file or directory from a <see cref="FileSystemInfo"/>.
        /// </summary>
        /// <param name="info">The <see cref="FileSystemInfo"/>.</param>
        /// <param name="rootPath">The root path of the file system.</param>
        /// <param name="includeChildren">If set to <see langword="true"/>, include the children if this is a directory.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/> instance for the local file or directory.
        /// </returns>
        public static JumboFileSystemEntry FromFileSystemInfo(FileSystemInfo info, string? rootPath, bool includeChildren)
        {
            ArgumentNullException.ThrowIfNull(info);

            var file = info as FileInfo;
            if (file != null)
                return JumboFile.FromFileInfo(file, rootPath);
            else
                return JumboDirectory.FromDirectoryInfo((DirectoryInfo)info, rootPath, includeChildren);
        }

        internal void Serialize(BinaryWriter writer)
        {
            writer.Write(_fullPath);
            writer.Write(_name);
            ValueWriter.WriteValue(_dateCreated, writer);
        }

        internal static string StripRootPath(string fullPath, string? rootPath)
        {
            if (rootPath == null)
                return fullPath;

            if (!fullPath.StartsWith(rootPath, StringComparison.Ordinal))
                throw new ArgumentException("Invalid path.");

            var length = rootPath.Length;
            if (rootPath[rootPath.Length - 1] == Path.DirectorySeparatorChar || rootPath[rootPath.Length - 1] == Path.AltDirectorySeparatorChar)
                length -= 1;
            if (fullPath.Length == length)
                return Path.DirectorySeparatorChar.ToString(CultureInfo.InvariantCulture); // Root
            else
                return fullPath.Substring(length);
        }
    }
}
