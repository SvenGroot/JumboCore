// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Security.Cryptography;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Represents the header of a record file.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   A record file is a plain file containing a sequence of records, with an occasional record marker to make it possible to start reading
    ///   at any point in the file and find the next record marker so you can start reading from the beginning of a record.
    /// </para>
    /// <para>
    ///   Record markers are 16 bytes random values created with the cryptographic <see cref="System.Security.Cryptography.RandomNumberGenerator"/>
    ///   for maximum entropy.
    /// </para>
    /// <para>
    ///   A record file begins with a header, which uses the following format:
    /// </para>
    /// <list type="table">
    ///   <listheader>
    ///     <term>Field</term>
    ///     <description>Value</description>
    ///   </listheader>
    ///   <item>
    ///     <term>Version</term>
    ///     <description>Three bytes containing the ASCII string "REC", followed by a single byte containing the version number of the record file format.</description>
    ///   </item>
    ///   <item>
    ///     <term>Record type name</term>
    ///     <description>The assembly qualified name of the type of the records, as a utf-8 encoded string, preceded by the string length (this is the format used by <see cref="System.IO.BinaryWriter.Write(string)"/>.</description>
    ///   </item>
    ///   <item>
    ///     <term>Record marker</term>
    ///     <description>A 16-byte value specifying the record marker for this file.</description>
    ///   </item>
    /// </list>
    /// <para>
    ///   After the header, a sequence of records follows. Each record is preceded by a 4 byte record prefix which is an integer in little endian format.
    ///   If this prefix is -1, it is followed by a record marker, and not a record. Otherwise the prefix is followed by a record, which will be read by
    ///   the record types <see cref="IWritable.Read"/> implementation.
    /// </para>
    /// <para>
    ///   In future versions, the record prefix may be used for other information (such as the record size), but currently it will always be either -1 to indicate
    ///   a record marker, or 0 to indicate a record.
    /// </para>
    /// </remarks>
    public sealed class RecordFileHeader : IWritable
    {
        private static readonly byte[] _headerStart = new[] { (byte)'R', (byte)'E', (byte)'C', RecordFile.CurrentVersion };
        private Type? _recordType;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordFileHeader"/> class using the latest version and specified record type.
        /// </summary>
        /// <param name="recordType">The type of the records.</param>
        /// <param name="useStrongName"><see langword="true"/> to use the strong name of the assembly (if it has one) in the header; <see langword="false"/>
        /// to use the simple name.</param>
        public RecordFileHeader(Type recordType, bool useStrongName)
        {
            ArgumentNullException.ThrowIfNull(recordType);

            Version = RecordFile.CurrentVersion;
            if (useStrongName)
                RecordTypeName = recordType.AssemblyQualifiedName!;
            else
                RecordTypeName = string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}, {1}", recordType.FullName, recordType.Assembly.GetName().Name);
            _recordType = recordType;
            RecordMarker = GenerateRecordMarker();
        }

        /// <summary>
        /// Gets the version number of the record file format.
        /// </summary>
        public int Version { get; private set; }

        /// <summary>
        /// Gets or sets the name of the type of the records in the file.
        /// </summary>
        public string RecordTypeName { get; private set; }

        internal byte[] RecordMarker { get; private set; }

        /// <summary>
        /// Gets the type of the records in the file.
        /// </summary>
        public Type RecordType
        {
            get
            {
                if (_recordType == null)
                    _recordType = Type.GetType(RecordTypeName, true)!;
                return _recordType;
            }
        }

        /// <summary>
        /// Gets the record marker for this record file.
        /// </summary>
        /// <returns>A copy of the record marker for the file.</returns>
        public byte[] GetRecordMarker()
        {
            // This is not a property because it copies the array.
            var markerCopy = new byte[RecordFile.RecordMarkerSize];
            RecordMarker.CopyTo(markerCopy, 0);
            return markerCopy;
        }

        private static byte[] GenerateRecordMarker()
        {
            using (var rng = RandomNumberGenerator.Create())
            {
                var boundary = new byte[RecordFile.RecordMarkerSize];
                rng.GetBytes(boundary);
                return boundary;
            }
        }

        #region IWritable Members

        void IWritable.Write(System.IO.BinaryWriter writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            writer.Write(_headerStart);
            writer.Write(RecordTypeName);
            writer.Write(RecordMarker);
        }

        void IWritable.Read(System.IO.BinaryReader reader)
        {
            ArgumentNullException.ThrowIfNull(reader);

            var headerStart = reader.ReadBytes(_headerStart.Length);
            if (!(headerStart[0] == _headerStart[0] &&
                  headerStart[1] == _headerStart[1] &&
                  headerStart[2] == _headerStart[2]))
                throw new InvalidOperationException("The specified file is not a record file.");
            if (headerStart[3] != _headerStart[3])
                throw new InvalidOperationException("The specified record file uses an unsupported version.");

            Version = headerStart[3];
            RecordTypeName = reader.ReadString();
            RecordMarker = reader.ReadBytes(RecordFile.RecordMarkerSize);
        }

        #endregion
    }
}
