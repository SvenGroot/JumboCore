// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Ookii.Jumbo.Dfs.FileSystem;

namespace NameServerApplication
{
    /// <summary>
    /// Represents a directory in the distributed file system namespace.
    /// </summary>
    class DfsDirectory : DfsFileSystemEntry
    {
        private readonly List<DfsFileSystemEntry> _children = new List<DfsFileSystemEntry>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsDirectory"/> class.
        /// </summary>
        /// <param name="parent">The parent of the directory. May be <see langword="null" />.</param>
        /// <param name="name">The name of the directory.</param>
        /// <param name="dateCreated">The date the directory was created.</param>
        public DfsDirectory(DfsDirectory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
        }

        /// <summary>
        /// Gets the child directories and files of this directory.
        /// </summary>
        public IList<DfsFileSystemEntry> Children
        {
            get { return _children; }
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
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            writer.WriteLine("Directory listing for {0}", FullPath);
            writer.WriteLine();

            if (Children.Count == 0)
                writer.WriteLine("No entries.");
            else
            {
                foreach (var entry in Children)
                    writer.WriteLine(entry.ToString());
            }
        }

        /// <summary>
        /// Saves this <see cref="DfsFileSystemEntry"/> to a file system image.
        /// </summary>
        /// <param name="writer">A <see cref="BinaryWriter"/> used to write to the file system image.</param>
        public override void SaveToFileSystemImage(BinaryWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            base.SaveToFileSystemImage(writer);
            writer.Write(Children.Count);
            foreach (var entry in Children)
                entry.SaveToFileSystemImage(writer);
        }

        /// <summary>
        /// Reads information about the <see cref="DfsDirectory"/> from the file system image.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> used to read the file system image.</param>
        /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
        protected override void LoadFromFileSystemImage(BinaryReader reader, Action<long> notifyFileSizeCallback)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));
            var childCount = reader.ReadInt32();
            _children.Clear();
            _children.Capacity = childCount;
            for (var x = 0; x < childCount; ++x)
            {
                // The FileSystemEntry constructor adds it to the Children collection, no need to do that here.
                DfsFileSystemEntry.LoadFromFileSystemImage(reader, this, notifyFileSizeCallback);
            }
        }

        /// <summary>
        /// Creates a <see cref="JumboFileSystemEntry"/> from this <see cref="DfsFileSystemEntry"/>.
        /// </summary>
        /// <param name="includeChildren">If set to <see langword="true"/> include the children if this is a directory.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/>.
        /// </returns>
        public override JumboFileSystemEntry ToJumboFileSystemEntry(bool includeChildren = true)
        {
            return ToJumboDirectory(includeChildren);
        }

        /// <summary>
        /// Creates a <see cref="JumboDirectory"/> from this <see cref="DfsDirectory"/>.
        /// </summary>
        /// <param name="includeChildren">If set to <see langword="true"/> include the children of the directory.</param>
        /// <returns>A <see cref="JumboDirectory"/>.</returns>
        public JumboDirectory ToJumboDirectory(bool includeChildren = true)
        {
            // Create a shallow copy: don't include children of child directories.
            return new JumboDirectory(FullPath, Name, DateCreated, includeChildren ? Children.Select(e => e.ToJumboFileSystemEntry(false)) : null);
        }
    }
}
