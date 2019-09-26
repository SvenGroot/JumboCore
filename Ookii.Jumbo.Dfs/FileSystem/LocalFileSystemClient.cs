// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Globalization;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Provides access to the local file system.
    /// </summary>
    public sealed class LocalFileSystemClient : FileSystemClient
    {
        private static readonly LocalPathUtility _path = new LocalPathUtility();
        private readonly string _rootPath;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalFileSystemClient"/> class.
        /// </summary>
        public LocalFileSystemClient()
            : base(CreateLocalConfiguration(null))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LocalFileSystemClient"/> class with the specified root path.
        /// </summary>
        /// <param name="rootPath">The root path.</param>
        /// <remarks>
        /// <para>
        ///   In this configuration, all absolute paths (even if they contain a drive specifier on Windows) passed to the members of this class will be made relative to the
        ///   specified root path. Note that the <see cref="Path"/> property doesn't consider the root path.
        /// </para>
        /// </remarks>
        public LocalFileSystemClient(string rootPath)
            : base(CreateLocalConfiguration(rootPath))
        {
            if( rootPath == null )
                throw new ArgumentNullException(nameof(rootPath));
            if( !Directory.Exists(rootPath) )
                throw new DirectoryNotFoundException(string.Format(CultureInfo.InvariantCulture, "The root directory '{0}' does not exist.", rootPath));

            _rootPath = rootPath;
        }

        internal LocalFileSystemClient(DfsConfiguration configuration)
            : base(configuration)
        {
            // HostName is always a file:// URI when this method is used.
            if( configuration.FileSystem.Url.AbsolutePath != "/" )
                _rootPath = System.IO.Path.GetFullPath(Uri.UnescapeDataString(configuration.FileSystem.Url.AbsolutePath));
        }

        /// <summary>
        /// Gets the root path of the file system.
        /// </summary>
        /// <value>
        /// The root path of the file system, or <see langword="null"/> if absolute paths are accepted as-is.
        /// </value>
        public string RootPath
        {
            get { return _rootPath; }
        }

        /// <summary>
        /// Gets the path utility for this file system.
        /// </summary>
        /// <value>
        /// The <see cref="IFileSystemPathUtility"/> implementation for this file system.
        /// </value>
        public override IFileSystemPathUtility Path
        {
            get { return _path; }
        }

        /// <summary>
        /// Gets the default block size for the file system.
        /// </summary>
        /// <value>
        /// Always returns <see langword="null"/>, because the local file system doesn't support blocks.
        /// </value>
        public override int? DefaultBlockSize
        {
            get { return null; }
        }

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>
        /// A <see cref="JumboDirectory"/> object representing the directory.
        /// </returns>
        public override JumboDirectory GetDirectoryInfo(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            DirectoryInfo directory = new DirectoryInfo(AdjustPath(path));
            return directory.Exists ? JumboDirectory.FromDirectoryInfo(directory, RootPath) : null;
        }

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>
        /// A <see cref="JumboFile"/> object referring to the file.
        /// </returns>
        public override JumboFile GetFileInfo(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            FileInfo file = new FileInfo(AdjustPath(path));
            return file.Exists ? JumboFile.FromFileInfo(file, RootPath) : null;
        }

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/> object referring to the file or directory, or <see langword="null"/> if the .
        /// </returns>
        public override JumboFileSystemEntry GetFileSystemEntryInfo(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            path = AdjustPath(path);
            FileInfo file = new FileInfo(path);
            if( file.Exists )
                return JumboFile.FromFileInfo(file, RootPath);

            DirectoryInfo directory = new DirectoryInfo(path);
            if( directory.Exists )
                return JumboDirectory.FromDirectoryInfo(directory, RootPath);

            return null;
        }

        /// <summary>
        /// Creates the specified directory in the file system.
        /// </summary>
        /// <param name="path">The path of the directory to create.</param>
        public override void CreateDirectory(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            path = AdjustPath(path);
            Directory.CreateDirectory(path);
        }

        /// <summary>
        /// Opens the specified file on the file system for reading.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to read the contents of the file.
        /// </returns>
        public override Stream OpenFile(string path)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            path = AdjustPath(path);
            return File.OpenRead(path);
        }

        /// <summary>
        /// Creates a new file with the specified path on the file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">Ignored.</param>
        /// <param name="replicationFactor">Ignored.</param>
        /// <param name="useLocalReplica">Ignored.</param>
        /// <param name="recordOptions">Ignored.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to write data to the file.
        /// </returns>
        public override Stream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, IO.RecordStreamOptions recordOptions)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            path = AdjustPath(path);
            return File.Create(path);
        }

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns>
        ///   <see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.
        /// </returns>
        public override bool Delete(string path, bool recursive)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            path = AdjustPath(path);
            if( File.Exists(path) )
            {
                File.Delete(path);
                return true;
            }
            else if( Directory.Exists(path) )
            {
                Directory.Delete(path, recursive);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Moves the specified file or directory.
        /// </summary>
        /// <param name="source">The path of the file or directory to move.</param>
        /// <param name="destination">The path to move the entry to.</param>
        public override void Move(string source, string destination)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));

            source = AdjustPath(source);
            destination = AdjustPath(destination);
            if( source == null )
                throw new ArgumentNullException(nameof(source));
            if( destination == null )
                throw new ArgumentNullException(nameof(destination));

            // This is the way the DFS behaves, so we need to mimic that.
            if( Directory.Exists(destination) )
                destination = System.IO.Path.Combine(destination, System.IO.Path.GetFileName(source));

            if( File.Exists(source) )
                File.Move(source, destination);
            else
                Directory.Move(source, destination);
        }

        private static DfsConfiguration CreateLocalConfiguration(string rootPath)
        {
            DfsConfiguration config = new DfsConfiguration();
            config.FileSystem.Url = rootPath == null ? new Uri("file:///") : new Uri(new Uri("file:///"), rootPath);
            return config;
        }

        private string AdjustPath(string path)
        {
            if( _rootPath == null )
                return path;
            else
            {
                if( System.IO.Path.IsPathRooted(path) )
                {
                    int rootLength = System.IO.Path.GetPathRoot(path).Length;
                    path = path.Substring(rootLength);
                }
                return System.IO.Path.Combine(_rootPath, path);
            }
        }
    }
}
