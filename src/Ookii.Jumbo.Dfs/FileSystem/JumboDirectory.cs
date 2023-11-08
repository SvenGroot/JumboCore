// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides information about a directory on a file system accessible using a <see cref="FileSystemClient"/>.
    /// </summary>

    [ValueWriter(typeof(Writer))]
    public sealed class JumboDirectory : JumboFileSystemEntry
    {
        #region Nested types

        /// <summary>
        /// The value writer for <see cref="JumboDirectory"/>.
        /// </summary>
        public class Writer : IValueWriter<JumboDirectory>
        {
            /// <inheritdoc/>
            public JumboDirectory Read(BinaryReader reader)
                => new(reader);

            /// <inheritdoc/>
            public void Write(JumboDirectory value, BinaryWriter writer)
            {
                ArgumentNullException.ThrowIfNull(nameof(value));
                ArgumentNullException.ThrowIfNull(nameof(writer));
                value.Serialize(writer);
                ValueWriter.WriteValue(value.Children, writer);
            }
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="JumboDirectory"/> class.
        /// </summary>
        /// <param name="fullPath">The full path or the directory.</param>
        /// <param name="name">The name of the directory.</param>
        /// <param name="dateCreated">The date the directory was created.</param>
        /// <param name="children">The children of the directory. May be <see langword="null"/>.</param>
        public JumboDirectory(string fullPath, string name, DateTime dateCreated, IEnumerable<JumboFileSystemEntry>? children)
            : base(fullPath, name, dateCreated)
        {
            Children = children?.ToImmutableArray() ?? ImmutableArray<JumboFileSystemEntry>.Empty;
        }

        private JumboDirectory(BinaryReader reader)
            : base(reader)
        {
            Children = ValueWriter<ImmutableArray<JumboFileSystemEntry>>.ReadValue(reader);
        }

        /// <summary>
        /// Gets the files and directories contained in this directory.
        /// </summary>
        /// <value>
        /// A list of <see cref="JumboFileSystemEntry"/> instances for the children of the directory.
        /// </value>
        /// <remarks>
        /// Depending on how this <see cref="JumboDirectory"/> instance was obtained, this collection may not be filled.
        /// </remarks>
        public ImmutableArray<JumboFileSystemEntry> Children { get; }

        /// <summary>
        /// Creates a <see cref="JumboDirectory"/> instance for a local directory from a <see cref="DirectoryInfo"/>.
        /// </summary>
        /// <param name="directory">The <see cref="DirectoryInfo"/>.</param>
        /// <param name="rootPath">The root path of the file system.</param>
        /// <returns>
        /// A <see cref="JumboDirectory"/> instance for the local directory.
        /// </returns>
        public static JumboDirectory FromDirectoryInfo(DirectoryInfo directory, string? rootPath)
        {
            return FromDirectoryInfo(directory, rootPath, true);
        }

        /// <summary>
        /// Creates a <see cref="JumboDirectory"/> instance for a local directory from a <see cref="DirectoryInfo"/>.
        /// </summary>
        /// <param name="directory">The <see cref="DirectoryInfo"/>.</param>
        /// <param name="rootPath">The root path of the file system.</param>
        /// <param name="includeChildren">If set to <see langword="true"/>, the children of the directory are included.</param>
        /// <returns>
        /// A <see cref="JumboDirectory"/> instance for the local directory.
        /// </returns>
        public static JumboDirectory FromDirectoryInfo(DirectoryInfo directory, string? rootPath, bool includeChildren)
        {
            ArgumentNullException.ThrowIfNull(directory);
            if (!directory.Exists)
                throw new DirectoryNotFoundException(string.Format(CultureInfo.CurrentCulture, "The directory '{0}' does not exist.", directory.FullName));

            IEnumerable<JumboFileSystemEntry>? children = null;
            if (includeChildren)
                children = directory.GetFileSystemInfos().Select(info => JumboFileSystemEntry.FromFileSystemInfo(info, rootPath, false));
            var fullPath = StripRootPath(directory.FullName, rootPath);
            return new JumboDirectory(fullPath, fullPath.Length == 1 ? "" : directory.Name, directory.CreationTimeUtc, children);
        }

        /// <summary>
        /// Gets the child with the specified name.
        /// </summary>
        /// <param name="name">The name of the child.</param>
        /// <returns>The <see cref="JumboFileSystemEntry"/> for the child, or <see langword="null"/> if it doesn't exist.</returns>
        public JumboFileSystemEntry? GetChild(string name)
        {
            ArgumentNullException.ThrowIfNull(name);
            return Children.Where(child => child.Name == name).SingleOrDefault();
        }

        /// <summary>
        /// Gets a string representation of this directory.
        /// </summary>
        /// <returns>A string representation of this directory.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, ListingEntryFormat, DateCreated.ToLocalTime(), "<DIR>", Name);
        }

        /// <summary>
        /// Prints a listing of the directory.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> </param>
        public void PrintListing(TextWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.WriteLine("Directory listing for {0}", FullPath);
            writer.WriteLine();

            if (Children.Length == 0)
                writer.WriteLine("No entries.");
            else
            {
                foreach (var entry in Children)
                    writer.WriteLine(entry.ToString());
            }
        }
    }
}
