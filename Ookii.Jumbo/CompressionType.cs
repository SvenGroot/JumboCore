// Copyright (c) Sven Groot (Ookii.org)

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides the types of compression supported by Jumbo.
    /// </summary>
    public enum CompressionType
    {
        /// <summary>
        /// Don't use compression.
        /// </summary>
        None,
        /// <summary>
        /// Use the <see cref="System.IO.Compression.GZipStream"/> class for compression.
        /// </summary>
        GZip
    }
}
