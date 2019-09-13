// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Interface for input streams that offer special handling of records.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix", Justification = "Interface should only be applied to streams.")]
    public interface IRecordInputStream
    {
        /// <summary>
        /// Gets the record options applied to this stream.
        /// </summary>
        /// <value>One or more of the <see cref="RecordStreamOptions"/> values.</value>
        RecordStreamOptions RecordOptions { get; }

        /// <summary>
        /// Gets or sets the position in the stream after which no data will be read.
        /// </summary>
        /// <value>
        /// 	The position after which <see cref="System.IO.Stream.Read"/> method will not return any data. The default value is the length of the stream.
        /// </value>
        /// <remarks>
        /// <para>
        ///   For a stream where <see cref="RecordOptions"/> is set to <see cref="RecordStreamOptions.DoNotCrossBoundary"/> you can use this property
        ///   to ensure that no data after the boundary is read if you only wish to read records up to the boundary.
        /// </para>
        /// <para>
        ///   On the Jumbo DFS, crossing a block boundary will cause a network connection to be established and data to be read from
        ///   a different data server. If you are reading records from only a single block (as is often the case for Jumbo Jet tasks)
        ///   this property can be used to ensure that no data from the next block will be read.
        /// </para>
        /// <para>
        ///   Setting this property to a value other than the stream length if <see cref="RecordStreamOptions.DoNotCrossBoundary"/> is not set, or
        ///   to a value that is not on a structural boundary can cause reading to halt in the middle of a record, and is therefore not recommended.
        /// </para>
        /// </remarks>
        long StopReadingAtPosition { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance has stopped reading.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if the stream has reached the position indicated by <see cref="StopReadingAtPosition"/>; otherwise, <see langword="false"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   If this property is <see langword="true"/> it means the next call to <see cref="System.IO.Stream.Read"/> will return 0.
        /// </para>
        /// </remarks>
        bool IsStopped { get; }

        /// <summary>
        /// Gets the amount of padding skipped while reading from the stream.
        /// </summary>
        /// <value>The amount of padding bytes skipped.</value>
        long PaddingBytesSkipped { get; }

        /// <summary>
        /// Determines the offset of the specified position from the directly preceding structural boundary (e.g. a block boundary on the DFS).
        /// </summary>
        /// <param name="position">The position.</param>
        /// <returns>
        /// 	The offset from the structural boundary that directly precedes the specified position.
        /// </returns>
        long OffsetFromBoundary(long position);

        /// <summary>
        /// Determines whether the range between two specified positions does not cross a structural boundary (e.g. a block boundary on the DFS).
        /// </summary>
        /// <param name="position1">The first position.</param>
        /// <param name="position2">The second position.</param>
        /// <returns>
        ///     <see langword="true"/> if the <paramref name="position1"/> and <paramref name="position2"/> fall inside the same boundaries (e.g. if
        ///     both positions are in the same block in the DFS); otherwise, <see langword="false"/>.
        /// </returns>
        bool AreInsideSameBoundary(long position1, long position2);
    }
}
