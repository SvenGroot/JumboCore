// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.ObjectModel;
using Ookii.Jumbo.IO;
using Ookii.Jumbo.Dfs.FileSystem;
using Ookii.Jumbo.Dfs;

namespace NameServerApplication
{
    /// <summary>
    /// Represents a file in the distributed file system.
    /// </summary>
    /// <remarks>
    /// When a client retrieves an instance of this class from the name server it will be a copy of the actual file record,
    /// so modifying any of the properties will not have any effect on the actual file system.
    /// </remarks>
    class DfsFile : DfsFileSystemEntry 
    {
        private readonly List<Guid> _blocks = new List<Guid>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DfsFile"/> class.
        /// </summary>
        /// <param name="parent">The parent of the file. May be <see langword="null"/>.</param>
        /// <param name="name">The name of the file.</param>
        /// <param name="dateCreated">The date the file was created.</param>
        /// <param name="blockSize">The size of the blocks of the file.</param>
        /// <param name="replicationFactor">The number of replicas to create of each block in the file.</param>
        /// <param name="recordOptions">The record options.</param>
        public DfsFile(DfsDirectory parent, string name, DateTime dateCreated, int blockSize, int replicationFactor, RecordStreamOptions recordOptions)
            : base(parent, name, dateCreated)
        {
            if( blockSize <= 0 )
                throw new ArgumentOutOfRangeException(nameof(blockSize), "File block size must be larger than zero.");
            if( blockSize % Packet.PacketSize != 0 )
                throw new ArgumentException("Block size must be a multiple of the packet size.", nameof(blockSize));
            if( replicationFactor <= 0 )
                throw new ArgumentOutOfRangeException(nameof(replicationFactor), "A block must have at least one replica.");

            BlockSize = blockSize;
            ReplicationFactor = replicationFactor;
            RecordOptions = recordOptions;
        }

        internal DfsFile(DfsDirectory parent, string name, DateTime dateCreated)
            : base(parent, name, dateCreated)
        {
            // This constructor is used by FileSystemEntry.LoadFromFileSystemImage, which will load the block size and replication factor from the image later.
        }

        /// <summary>
        /// Gets the list of blocks that make up this file.
        /// </summary>
        public IList<Guid> Blocks
        {
            get { return _blocks; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the file is held open for writing by a client.
        /// </summary>
        /// <remarks>
        /// Under the current implementation, this property can only be set to <see langword="true"/> when the file is
        /// created. Once the file is closed, it can never be set to <see langword="true"/> again.
        /// </remarks>
        public bool IsOpenForWriting { get; set; }

        /// <summary>
        /// Gets or sets the size of the file, in bytes.
        /// </summary>
        /// <remarks>
        /// Each block of the file will be the full block size, except the last block which is <see cref="Size"/> - (<see cref="Blocks"/>.Length * block size).
        /// </remarks>
        public long Size { get; set; }

        /// <summary>
        /// Gets the size of the blocks of the file.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Although most files will use the file system's default block size (configured on the name server), it is possible to override the block size on a per-file basis.
        /// </para>
        /// <para>
        ///   The block size is specified when the file is created, it cannot be changed afterwards.
        /// </para>
        /// </remarks>
        public int BlockSize { get; private set; }

        /// <summary>
        /// Gets the number of replicas created for the blocks of the file.
        /// </summary>
        /// <remarks>
        /// <para>
        ///   Although most files will use the file system's default replication factor (configured on the name server), it is possible to override it on a per-file basis.
        /// </para>
        /// <para>
        ///   The replication factor is specified when the file is created, it cannot be changed afterwards.
        /// </para>
        /// </remarks>
        public int ReplicationFactor { get; private set; }

        /// <summary>
        /// Gets or sets the record options applied to this file.
        /// </summary>
        /// <value>The record options.</value>
        public RecordStreamOptions RecordOptions { get; private set; }

        /// <summary>
        /// Saves this <see cref="DfsFileSystemEntry"/> to a file system image.
        /// </summary>
        /// <param name="writer">A <see cref="System.IO.BinaryWriter"/> used to write to the file system image.</param>
        public override void SaveToFileSystemImage(System.IO.BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException(nameof(writer));
            base.SaveToFileSystemImage(writer);
            writer.Write(Size);
            writer.Write(IsOpenForWriting);
            writer.Write(BlockSize);
            writer.Write(ReplicationFactor);
            writer.Write((int)RecordOptions);
            writer.Write(Blocks.Count);
            foreach( Guid block in Blocks )
                writer.Write(block.ToByteArray());
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
        /// Creates a <see cref="JumboFileSystemEntry"/> from this <see cref="DfsFileSystemEntry"/>.
        /// </summary>
        /// <param name="includeChildren">if set to <see langword="true"/> include the children if this is a directory.</param>
        /// <returns>
        /// A <see cref="JumboFileSystemEntry"/>.
        /// </returns>
        public override JumboFileSystemEntry ToJumboFileSystemEntry(bool includeChildren = true)
        {
            return ToJumboFile();
        }

        /// <summary>
        /// Creates a <see cref="JumboFile"/> from this <see cref="DfsFile"/>.
        /// </summary>
        /// <returns>A <see cref="JumboFile"/>.</returns>
        public JumboFile ToJumboFile()
        {
            return new JumboFile(FullPath, Name, DateCreated, Size, BlockSize, ReplicationFactor, RecordOptions, IsOpenForWriting, Blocks);
        }

        /// <summary>
        /// Prints information about the file.
        /// </summary>
        /// <param name="writer">The <see cref="System.IO.TextWriter"/> to write the information to.</param>
        public void PrintFileInfo(System.IO.TextWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException(nameof(writer));
            writer.WriteLine("Path:             {0}", FullPath);
            writer.WriteLine("Size:             {0:#,0} bytes", Size);
            writer.WriteLine("Block size:       {0:#,0} bytes", BlockSize);
            writer.WriteLine("Replicas:         {0}", ReplicationFactor);
            writer.WriteLine("Record options:   {0}", RecordOptions);
            writer.WriteLine("Open for writing: {0}", IsOpenForWriting);
            writer.WriteLine("Blocks:           {0}", Blocks.Count);
            foreach( Guid block in Blocks )
                writer.WriteLine("{{{0}}}", block);
        }

        /// <summary>
        /// Reads information about the <see cref="DfsFile"/> from the file system image.
        /// </summary>
        /// <param name="reader">The <see cref="System.IO.BinaryReader"/> used to read the file system image.</param>
        /// <param name="notifyFileSizeCallback">A function that should be called to notify the caller of the size of deserialized files.</param>
        protected override void LoadFromFileSystemImage(System.IO.BinaryReader reader, Action<long> notifyFileSizeCallback)
        {
            if( reader == null )
                throw new ArgumentNullException(nameof(reader));
            Size = reader.ReadInt64();
            IsOpenForWriting = reader.ReadBoolean();
            BlockSize = reader.ReadInt32();
            ReplicationFactor = reader.ReadInt32();
            RecordOptions = (RecordStreamOptions)reader.ReadInt32();
            int blockCount = reader.ReadInt32();
            _blocks.Clear();
            _blocks.Capacity = blockCount;
            for( int x = 0; x < blockCount; ++x )
            {
                _blocks.Add(new Guid(reader.ReadBytes(16)));
            }

            if( notifyFileSizeCallback != null )
                notifyFileSizeCallback(Size);
        }
    }
}
