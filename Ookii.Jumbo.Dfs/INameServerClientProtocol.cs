// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs.FileSystem;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Defines the interface used by clients to communicate with the NameServer.
    /// </summary>
    public interface INameServerClientProtocol
    {
        /// <summary>
        /// Creates the specified directory in the distributed file system.
        /// </summary>
        /// <param name="path">The path of the directory to create.</param>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        void CreateDirectory(string path);

        /// <summary>
        /// Gets information about a directory in the file system.
        /// </summary>
        /// <param name="path">The full path of the directory.</param>
        /// <returns>A <see cref="JumboDirectory"/> object representing the directory, or <see langword="null"/> if the directory doesn't exist.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        JumboDirectory GetDirectoryInfo(string path);

        /// <summary>
        /// Creates a new file in the specified directory.
        /// </summary>
        /// <param name="path">The full path of the new file.</param>
        /// <param name="blockSize">The size of the blocks in the file, or zero to use the file system default block size.</param>
        /// <param name="replicationFactor">The number of replicas to create of the file's blocks, or zero to use the file system default replication factor.</param>
        /// <param name="localReplica"><see langword="true"/> to put the first replica on the node that's creating the file if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <param name="recordOptions">The record options for the file.</param>
        /// <returns>
        /// The <see cref="BlockAssignment"/> for the first block of the file.
        /// </returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, contains a file name, or refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">Part of the path specified in <paramref name="path"/> does not exist.</exception>
        BlockAssignment CreateFile(string path,  int blockSize, int replicationFactor, bool localReplica, RecordStreamOptions recordOptions);

        /// <summary>
        /// Deletes the specified file or directory.
        /// </summary>
        /// <param name="path">The path of the file or directory to delete.</param>
        /// <param name="recursive"><see langword="true"/> to delete all children if <paramref name="path"/> refers to a directory; otherwise <see langword="false"/>.</param>
        /// <returns><see langword="true"/> if the file was deleted; <see langword="false"/> if it doesn't exist.</returns>
        bool Delete(string path, bool recursive);

        /// <summary>
        /// Moves the specified file or directory.
        /// </summary>
        /// <param name="source">The path of the file or directory to move.</param>
        /// <param name="destination">The path to move the entry to.</param>
        void Move(string source, string destination);

        /// <summary>
        /// Gets information about a file.
        /// </summary>
        /// <param name="path">The full path of the file.</param>
        /// <returns>A <see cref="JumboFile"/> object referring to the file, or <see langword="null"/> if the file doesn't exist.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        JumboFile GetFileInfo(string path);

        /// <summary>
        /// Gets information about a file or directory.
        /// </summary>
        /// <param name="path">The full path of the file or directory.</param>
        /// <returns>A <see cref="JumboFileSystemEntry"/> object referring to the file or directory, or <see langword="null" /> if the entry doesn't exist.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" />.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, or contains a file name.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">One of the parent directories in the path specified in <paramref name="path"/> does not exist.</exception>
        JumboFileSystemEntry GetFileSystemEntryInfo(string path);

        /// <summary>
        /// Appends a new block to a file.
        /// </summary>
        /// <param name="path">The full path of the file to which to append a block.</param>
        /// <param name="useLocalReplica"><see langword="true"/> to put the first replica on the node that's appending the block if it's part of the DFS cluster; otherwise, <see langword="false"/>.</param>
        /// <returns>Information about the new block.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="path"/> is <see langword="null" /> an empty string.</exception>
        /// <exception cref="ArgumentException"><paramref name="path"/> is not an absolute path, contains an empty component, contains a file name, or refers to an existing file or directory.</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">Part of <paramref name="path"/> does not exist.</exception>
        BlockAssignment AppendBlock(string path, bool useLocalReplica);

        /// <summary>
        /// Closes a file that is open for writing.
        /// </summary>
        /// <param name="path">The path of the file to close.</param>
        void CloseFile(string path);

        /// <summary>
        /// Gets the list of data servers that have the specified block.
        /// </summary>
        /// <param name="blockId">The <see cref="Guid"/> identifying the block.</param>
        /// <returns>A list of <see cref="ServerAddress"/> objects that give the addresses of the servers that have this block.</returns>
        ServerAddress[] GetDataServersForBlock(Guid blockId);

        /// <summary>
        /// Gets the file that the specified block belongs to.
        /// </summary>
        /// <param name="blockId">The block id.</param>
        /// <returns>The path of the file that the block belongs to, or <see langword="null"/> if the specified block ID isn't known to the DFS.</returns>
        string GetFileForBlock(Guid blockId);

        /// <summary>
        /// Gets the blocks known to the DFS.
        /// </summary>
        /// <param name="kind">The kind of blocks to include in the results.</param>
        /// <returns>A list of blocks.</returns>
        Guid[] GetBlocks(BlockKind kind);

        /// <summary>
        /// Gets current metrics for the distributed file system.
        /// </summary>
        /// <returns>An object holding the metrics for the name server.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate")]
        DfsMetrics GetMetrics();

        /// <summary>
        /// Gets the number of blocks from the specified block list that the data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to check.</param>
        /// <param name="blocks">The blocks to check for.</param>
        /// <returns>The number of blocks.</returns>
        /// <remarks>
        /// This function returns the number of items in the intersection of <paramref name="blocks"/>
        /// and the block list for the specified server.
        /// </remarks>
        int GetDataServerBlockCount(ServerAddress dataServer, Guid[] blocks);

        /// <summary>
        /// Gets the list of blocks that a particular data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to return.</param>
        /// <returns>The block IDs of all the blocks on that server.</returns>
        Guid[] GetDataServerBlocks(ServerAddress dataServer);

        /// <summary>
        /// Gets the list of blocks, out of the specified blocks, that a particular data server has.
        /// </summary>
        /// <param name="dataServer">The data server whose blocks to return.</param>
        /// <param name="blocks">The list of blocks to filter by.</param>
        /// <returns>The block IDs of all the blocks on that server.</returns>
        Guid[] GetDataServerBlocksFromList(ServerAddress dataServer, Guid[] blocks);

        /// <summary>
        /// Gets the contents of the diagnostic log file.
        /// </summary>
        /// <param name="kind">The kind of log file to return.</param>
        /// <param name="maxSize">The maximum number of bytes to return.</param>
        /// <returns>The contents of the diagnostic log file.</returns>
        /// <remarks>
        /// If the log file is larger than <paramref name="maxSize"/>, the tail of the file up to the
        /// specified size is returned.
        /// </remarks>
        string GetLogFileContents(LogFileKind kind, int maxSize);

        /// <summary>
        /// Removes the specified data server from the name server's list of known data servers.
        /// </summary>
        /// <param name="dataServer">The address of the data server to remove.</param>
        /// <remarks>
        /// <para>
        ///   If a data server has been shutdown, and is known not to restart soon, you can use this function to remove it
        ///   immediately rather than waiting for the timeout to expire. The name server will remove all information regarding
        ///   to the data server and force an immediate replication check.
        /// </para>
        /// </remarks>
        void RemoveDataServer(ServerAddress dataServer);

        /// <summary>
        /// Immediately creates a checkpoint of the file system namespace.
        /// </summary>
        void CreateCheckpoint();

        /// <summary>
        /// Gets or sets a value that indicates whether safe mode is on or off.
        /// </summary>
        /// <value>
        /// <see langword="true" /> if safe mode is on; otherwise, <see langword="false" />.
        /// </value>
        /// <remarks>
        /// Disabling safe mode before full replication is achieved will cause an immediate replication check.
        /// </remarks>
        bool SafeMode { get; set; }

        /// <summary>
        /// Gets the default size of a single block in a file.
        /// </summary>
        /// <value>
        /// The default size of a single block in a file.
        /// </value>
        /// <remarks>
        /// All blocks in a file except the last one will be exactly the block size. Individual files may override the
        /// default block size.
        /// </remarks>
        int BlockSize { get; }
    }
}
