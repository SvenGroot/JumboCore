// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Indicates how the <see cref="FileOutputChannel"/> writes intermediate data to the disk.
    /// </summary>
    public enum FileChannelOutputType
    {
        /// <summary>
        /// A <see cref="SpillRecordWriter{T}"/> is used to write the output. The intermediate data will be a single file
        /// containing multiple regions for each partition. The data does not contain record size markers.
        /// </summary>
        Spill,
        /// <summary>
        /// A <see cref="SortSpillRecordWriter{T}"/> is used to write the output. The intermediate data will be a single file
        /// containing one region for each partition, and the data in each region is sorted. The data contains record size markers
        /// so it can be read using the <see cref="Ookii.Jumbo.IO.RawRecord"/> class, allowing for raw comparisons on the receiving
        /// side on the channel.
        /// </summary>
        SortSpill
    }
}
