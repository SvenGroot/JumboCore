﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Ookii.Jumbo.IO;
using System.Globalization;
using System.Collections.Concurrent;

namespace Ookii.Jumbo.Dfs.FileSystem
{
    /// <summary>
    /// Abstract base class for a class providing file system functionality.
    /// </summary>
    public abstract class FileSystemClient
    {
        private static readonly ConcurrentDictionary<string, Type> _fileSystemTypes = new ConcurrentDictionary<string,Type>(StringComparer.OrdinalIgnoreCase);

        private readonly DfsConfiguration _configuration;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileSystemClient" /> class.
        /// </summary>
        /// <param name="configuration">The <see cref="DfsConfiguration"/> for the file system.</param>
        protected FileSystemClient(DfsConfiguration configuration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");
            _configuration = configuration;
        }

        /// <summary>
        /// Gets the path utility for this file system.
        /// </summary>
        /// <value>
        /// The <see cref="IFileSystemPathUtility"/> implementation for this file system.
        /// </value>
        public abstract IFileSystemPathUtility Path { get; }

        /// <summary>
        /// Gets the default block size for the file system.
        /// </summary>
        /// <value>
        /// The default block size, or <see langword="null"/> if the file system doesn't support blocks.
        /// </value>
        public abstract int? DefaultBlockSize { get; }

        /// <summary>
        /// Gets the <see cref="DfsConfiguration"/> used to create this instance.
        /// </summary>
        /// <value>
        /// The <see cref="DfsConfiguration"/> used to create this instance.
        /// </value>
        public DfsConfiguration Configuration
        {
            get { return _configuration; }
        }

        /// <summary>
        /// Registers a file system for the specified scheme.
        /// </summary>
        /// <param name="scheme">The URL scheme.</param>
        /// <param name="fileSystemClientType">Type of the file system client.</param>
        public static void RegisterFileSystem(string scheme, Type fileSystemClientType)
        {
            if( scheme == null )
                throw new ArgumentNullException("scheme");
            if( fileSystemClientType == null )
                throw new ArgumentNullException("fileSystemClientType");
            if( string.IsNullOrWhiteSpace(scheme) )
                throw new ArgumentException("The scheme may not be empty.", "scheme");
            if( scheme.Equals("jdfs", StringComparison.OrdinalIgnoreCase) || scheme.Equals("file", StringComparison.OrdinalIgnoreCase) )
                throw new ArgumentException("You cannot replace the jdfs or file schemes.", "scheme");
            if( !fileSystemClientType.IsSubclassOf(typeof(FileSystemClient)) )
                throw new ArgumentException("The specified type does not derive from FileSystemClient.", "fileSystemClientType");

            _fileSystemTypes[scheme] = fileSystemClientType;
        }

        /// <summary>
        /// Creates a <see cref="FileSystemClient"/> instance with the specified configuration.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        /// <returns>An instance of a class deriving form <see cref="FileSystemClient"/>.</returns>
        public static FileSystemClient Create(DfsConfiguration configuration)
        {
            if( configuration == null )
                throw new ArgumentNullException("configuration");

            if( configuration.FileSystem.Url.Scheme == "file" )
                return new LocalFileSystemClient(configuration);
            else if( configuration.FileSystem.Url.Scheme == "jdfs" )
                return new DfsClient(configuration);
            else
            {
                Type type = _fileSystemTypes[configuration.FileSystem.Url.Scheme];

                return (FileSystemClient)Activator.CreateInstance(type, configuration);
            }
        }

        /// <summary>
        /// Creates a <see cref="FileSystemClient"/> instance.
        /// </summary>
        /// <returns>An instance of a class deriving form <see cref="FileSystemClient"/>.</returns>
        public static FileSystemClient Create()
        {
            return Create(DfsConfiguration.GetConfiguration());
        }

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>A <see cref="JumboDirectory"/> object representing the directory, or <see langword="null"/> if the directory doesn't exist.</returns>
        public abstract JumboDirectory GetDirectoryInfo(string path);

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="JumboFile"/> object referring to the file, or <see langword="null"/> if the file doesn't exist.</returns>
        public abstract JumboFile GetFileInfo(string path);

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>A <see cref="JumboFileSystemEntry"/> object referring to the file or directory, or <see langword="null" /> if the entry doesn't exist.</returns>
        public abstract JumboFileSystemEntry GetFileSystemEntryInfo(string path);

        /// <summary>
        /// Creates the specified directory in the file system.
        /// </summary>
        /// <param name="path">The path of the directory to create.</param>
        /// <remarks>
        /// If the directory specified by <paramref name="path"/> already exists, this function does nothing and no exception is thrown.
        /// </remarks>
        public abstract void CreateDirectory(string path);
        
        /// <summary>
        /// Uploads the contents of the specified stream to the file system.
        /// </summary>
        /// <param name="stream">The stream with the data to upload.</param>
        /// <param name="targetPath">The path of the file on the file system to write the data to.</param>
        public void UploadStream(Stream stream, string targetPath)
        {
            UploadStream(stream, targetPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads the contents of the specified stream to the file system.
        /// </summary>
        /// <param name="stream">The stream with the data to upload.</param>
        /// <param name="targetPath">The path of the file on the file system to write the data to.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>. This parameter will be ignored if the file system doesn't support replica placement.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadStream(Stream stream, string targetPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( targetPath == null )
                throw new ArgumentNullException("targetPath");
            if( stream == null )
                throw new ArgumentNullException("stream");

            using( Stream outputStream = CreateFile(targetPath, blockSize, replicationFactor, useLocalReplica, IO.RecordStreamOptions.None) )
            {
                CopyStream(targetPath, stream, outputStream, progressCallback);
            }
        }

        /// <summary>
        /// Uploads a file to the file system.
        /// </summary>
        /// <param name="localSourcePath">The path of the file to upload.</param>
        /// <param name="targetPath">The path on the file system to store the file. If this is the name of an existing directory, the file
        /// will be stored in that directory.</param>
        public void UploadFile(string localSourcePath, string targetPath)
        {
            UploadFile(localSourcePath, targetPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads a file to the file system.
        /// </summary>
        /// <param name="localSourcePath">The path of the file to upload.</param>
        /// <param name="targetPath">The path on the file system to store the file. If this is the name of an existing directory, the file
        /// will be stored in that directory.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>. This parameter will be ignored if the file system doesn't support replica placement.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadFile(string localSourcePath, string targetPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( targetPath == null )
                throw new ArgumentNullException("targetPath");
            if( localSourcePath == null )
                throw new ArgumentNullException("localSourcePath");
            JumboDirectory dir = GetDirectoryInfo(targetPath);
            if( dir != null )
            {
                string fileName = System.IO.Path.GetFileName(localSourcePath);
                targetPath = Path.Combine(targetPath, fileName);
            }
            using( FileStream inputStream = File.OpenRead(localSourcePath) )
            {
                UploadStream(inputStream, targetPath, blockSize, replicationFactor, useLocalReplica, progressCallback);
            }
        }
        /// <summary>
        /// Uploads the files in the specified directory to the file system.
        /// </summary>
        /// <param name="localSourcePath">The path of the directory on the local file system containing the files to upload.</param>
        /// <param name="targetPath">The path of the directory on the file system where the files should be stored. This path must not
        /// refer to an existing directory.</param>
        public void UploadDirectory(string localSourcePath, string targetPath)
        {
            UploadDirectory(localSourcePath, targetPath, 0, 0, true, null);
        }

        /// <summary>
        /// Uploads the files in the specified directory to the file system.
        /// </summary>
        /// <param name="localSourcePath">The path of the directory on the local file system containing the files to upload.</param>
        /// <param name="targetPath">The path of the directory on the file system where the files should be stored. This path must not
        /// refer to an existing directory.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>. This parameter will be ignored if the file system doesn't support replica placement.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void UploadDirectory(string localSourcePath, string targetPath, int blockSize, int replicationFactor, bool useLocalReplica, ProgressCallback progressCallback)
        {
            if( localSourcePath == null )
                throw new ArgumentNullException("localSourcePath");
            if( targetPath == null )
                throw new ArgumentNullException("targetPath");

            string[] files = System.IO.Directory.GetFiles(localSourcePath);

            JumboDirectory directory = GetDirectoryInfo(targetPath);
            if( directory != null )
                throw new ArgumentException(string.Format(System.Globalization.CultureInfo.CurrentCulture, "Directory {0} already exists on the file system.", targetPath), "targetPath");
            CreateDirectory(targetPath);

            foreach( string file in files )
            {
                string targetFile = Path.Combine(targetPath, System.IO.Path.GetFileName(file));
                UploadFile(file, targetFile, blockSize, replicationFactor, useLocalReplica, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the specified file from the file system, saving it to the specified stream.
        /// </summary>
        /// <param name="sourcePath">The path of the file on the file system to download.</param>
        /// <param name="stream">The stream to save the file to.</param>
        public void DownloadStream(string sourcePath, Stream stream)
        {
            DownloadStream(sourcePath, stream, null);
        }

        /// <summary>
        /// Downloads the specified file from the file system, saving it to the specified stream.
        /// </summary>
        /// <param name="sourcePath">The path of the file on the file system to download.</param>
        /// <param name="stream">The stream to save the file to.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadStream(string sourcePath, Stream stream, ProgressCallback progressCallback)
        {
            if( sourcePath == null )
                throw new ArgumentNullException("sourcePath");
            if( stream == null )
                throw new ArgumentNullException("stream");
            using( Stream inputStream = OpenFile(sourcePath) )
            {
                CopyStream(sourcePath, inputStream, stream, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the specified file from the file system to the specified local file.
        /// </summary>
        /// <param name="sourcePath">The path of the file on the file system to download.</param>
        /// <param name="localTargetPath">The path of the file on the local file system to save the file to. If this is the
        /// name of an existing directory, the file will be downloaded to that directory.</param>
        public void DownloadFile(string sourcePath, string localTargetPath)
        {
            DownloadFile(sourcePath, localTargetPath, null);
        }

        /// <summary>
        /// Downloads the specified file from the file system to the specified local file.
        /// </summary>
        /// <param name="sourcePath">The path of the file on the file system to download.</param>
        /// <param name="localTargetPath">The path of the file on the local file system to save the file to. If this is the
        /// name of an existing directory, the file will be downloaded to that directory.</param>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadFile(string sourcePath, string localTargetPath, ProgressCallback progressCallback)
        {
            if( sourcePath == null )
                throw new ArgumentNullException("sourcePath");
            if( localTargetPath == null )
                throw new ArgumentNullException("localTargetPath");

            if( Directory.Exists(localTargetPath) )
            {
                string fileName = Path.GetFileName(sourcePath);
                localTargetPath = System.IO.Path.Combine(localTargetPath, fileName);
            }
            using( FileStream stream = File.Create(localTargetPath) )
            {
                DownloadStream(sourcePath, stream, progressCallback);
            }
        }

        /// <summary>
        /// Downloads the files in the specified directory on the file system.
        /// </summary>
        /// <param name="sourcePath">The directory on the file system to download.</param>
        /// <param name="localTargetPath">The local directory to store the files.</param>
        /// <remarks>
        /// This function is not recursive; it will only download the files that are direct children of the
        /// specified directory.
        /// </remarks>
        public void DownloadDirectory(string sourcePath, string localTargetPath)
        {
            DownloadDirectory(sourcePath, localTargetPath, null);
        }
        
        /// <summary>
        /// Downloads the files in the specified directory on the file system.
        /// </summary>
        /// <param name="sourcePath">The directory on the file system to download.</param>
        /// <param name="localTargetPath">The local directory to store the files.</param>
        /// <remarks>
        /// This function is not recursive; it will only download the files that are direct children of the
        /// specified directory.
        /// </remarks>
        /// <param name="progressCallback">The <see cref="ProgressCallback"/> that will be called to report progress of the operation. May be <see langword="null"/>.</param>
        public void DownloadDirectory(string sourcePath, string localTargetPath, ProgressCallback progressCallback)
        {
            if( sourcePath == null )
                throw new ArgumentNullException("sourcePath");
            if( localTargetPath == null )
                throw new ArgumentNullException("localTargetPath");

            JumboDirectory dir = GetDirectoryInfo(sourcePath);
            if( dir == null )
                throw new DfsException("The specified directory does not exist.");
            foreach( JumboFileSystemEntry entry in dir.Children )
            {
                JumboFile file = entry as JumboFile;
                if( file != null )
                {
                    string localFile = System.IO.Path.Combine(localTargetPath, file.Name);
                    DownloadFile(file.FullPath, localFile, progressCallback);
                }
            }
        }

        /// <summary>
        /// Opens the specified file on the file system for reading.
        /// </summary>
        /// <param name="path">The path of the file.</param>
        /// <returns>A <see cref="Stream"/> that can be used to read the contents of the file.</returns>
        public abstract Stream OpenFile(string path);

        /// <summary>
        /// Creates a new file with the specified path on the file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <returns>A <see cref="Stream"/> that can be used to write data to the file.</returns>
        public Stream CreateFile(string path)
        {
            return CreateFile(path, 0, 0, true, RecordStreamOptions.None);
        }

        /// <summary>
        /// Creates a new file with the specified path on the file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to write data to the file.
        /// </returns>
        public Stream CreateFile(string path, int blockSize, int replicationFactor)
        {
            return CreateFile(path, blockSize, replicationFactor, true, RecordStreamOptions.None);
        }

        /// <summary>
        /// Creates a new file with the specified path on the file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <param name="recordOptions">The record options for the file. This parameter will be ignored if the file system doesn't support record options.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to write data to the file.
        /// </returns>
        public Stream CreateFile(string path, int blockSize, int replicationFactor, RecordStreamOptions recordOptions)
        {
            return CreateFile(path, blockSize, replicationFactor, true, recordOptions);
        }

        /// <summary>
        /// Creates a new file with the specified path on the file system.
        /// </summary>
        /// <param name="path">The path containing the directory and name of the file to create.</param>
        /// <param name="blockSize">The block size of the file, or zero to use the file system default block size. This parameter will be ignored if the file system doesn't support blocks.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor. This parameter will be ignored if the file system doesn't support replication.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the file system cluster; otherwise, <see langword="false"/>. This parameter will be ignored if the file system doesn't support replica placement.</param>
        /// <param name="recordOptions">The record options for the file. This parameter will be ignored if the file system doesn't support record options.</param>
        /// <returns>
        /// A <see cref="Stream"/> that can be used to write data to the file.
        /// </returns>
        public abstract Stream CreateFile(string path, int blockSize, int replicationFactor, bool useLocalReplica, RecordStreamOptions recordOptions);

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        public abstract bool Delete(string path, bool recursive);

        /// <summary>
        /// Moves the specified file or directory.
        /// </summary>
        /// <param name="source">The path of the file or directory to move.</param>
        /// <param name="destination">The path to move the entry to.</param>
        public abstract void Move(string source, string destination);

        private static void CopyStream(string fileName, Stream inputStream, Stream outputStream, ProgressCallback progressCallback)
        {
            byte[] buffer = new byte[4096];
            int bytesRead;
            int prevPercentage = -1;
            float length = inputStream.Length;
            if( progressCallback != null )
                progressCallback(fileName, 0, 0L);
            while( (bytesRead = inputStream.Read(buffer, 0, buffer.Length)) != 0 )
            {
                int percentage = (int)((inputStream.Position / length) * 100);
                if( percentage > prevPercentage )
                {
                    prevPercentage = percentage;
                    if( progressCallback != null )
                        progressCallback(fileName, percentage, inputStream.Position);
                }
                outputStream.Write(buffer, 0, bytesRead);
            }
            if( progressCallback != null )
                progressCallback(fileName, 100, inputStream.Length);
        }
    }
}
