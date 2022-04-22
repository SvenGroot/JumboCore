// Copyright (c) Sven Groot (Ookii.org)
using System.IO;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.IO
{
    /// <summary>
    /// Writes <see cref="GenSortRecord"/> records to a stream.
    /// </summary>
    public class GenSortRecordWriter : StreamRecordWriter<GenSortRecord>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GenSortRecordWriter"/> class that writes to the specified stream.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        public GenSortRecordWriter(Stream stream)
            : base(stream)
        {
        }

        /// <summary>
        /// Writes a record.
        /// </summary>
        /// <param name="record">The record to write.</param>
        protected override void WriteRecordInternal(GenSortRecord record)
        {
            Stream.Write(record.RecordBuffer, 0, GenSortRecord.RecordSize);
            base.WriteRecordInternal(record);
        }
    }
}
