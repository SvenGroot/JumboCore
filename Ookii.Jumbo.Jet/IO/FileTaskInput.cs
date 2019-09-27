using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Ookii.Jumbo.Jet.IO
{
    /// <summary>
    /// Represents a task input for a stage using <see cref="FileDataInput"/>.
    /// </summary>
    public class FileTaskInput : ITaskInput
    {
        private readonly string[] _locations; // Note: don't serialize with IWritable!

        /// <summary>
        /// Initializes a new instance of the <see cref="FileTaskInput"/> class.
        /// </summary>
        /// <param name="path">The path of the file to use.</param>
        /// <param name="offset">The offset in the file specified by <paramref name="path"/> of the split.</param>
        /// <param name="size">The size of the split.</param>
        /// <param name="locations">The names of the nodes on which this split is local. May be <see langword="null"/>.</param>
        public FileTaskInput(string path, long offset, long size, IEnumerable<string> locations)
        {
            if( path == null )
                throw new ArgumentNullException(nameof(path));
            if( offset < 0 )
                throw new ArgumentOutOfRangeException(nameof(offset));
            if( size < 1 )
                throw new ArgumentOutOfRangeException(nameof(size));

            Path = path;
            Offset = offset;
            Size = size;
            if( locations != null )
                _locations = locations.ToArray();
        }

        /// <summary>
        /// Gets the path of the file for this split.
        /// </summary>
        /// <value>
        /// The path of the file for this split.
        /// </value>
        public string Path { get; private set; }

        /// <summary>
        /// Gets the offset at which the split begins.
        /// </summary>
        /// <value>
        /// The offset of the split.
        /// </value>
        public long Offset { get; private set; }

        /// <summary>
        /// Gets the size of the split.
        /// </summary>
        /// <value>
        /// The size of the split.
        /// </value>
        public long Size { get; private set; }

        /// <summary>
        /// Gets a list of host names of nodes for which this task's input is local.
        /// </summary>
        /// <value>
        /// The locations, or <see langword="null"/> if the input doesn't use locality.
        /// </value>
        public ICollection<string> Locations
        {
            get { return _locations; }
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            if( writer == null )
                throw new ArgumentNullException(nameof(writer));
            // Don't serialize _locations
            writer.Write(Path);
            writer.Write(Offset);
            writer.Write(Size);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(BinaryReader reader)
        {
            if( reader == null )
                throw new ArgumentNullException(nameof(reader));
            Path = reader.ReadString();
            Offset = reader.ReadInt64();
            Size = reader.ReadInt64();
        }
    }
}
