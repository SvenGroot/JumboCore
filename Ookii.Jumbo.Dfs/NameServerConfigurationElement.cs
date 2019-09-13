// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration information about the name server.
    /// </summary>
    public class NameServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the default size of a block in a file.
        /// </summary>
        /// <value>
        /// The default block size, in bytes. The default value is 64 megabytes.
        /// </value>
        [ConfigurationProperty("blockSize", DefaultValue = "64MB", IsRequired = false, IsKey = false)]
        public BinarySize BlockSize
        {
            get { return (BinarySize)this["blockSize"]; }
            set { this["blockSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the number of replicas to maintain of each block.
        /// </summary>
        /// <value>
        /// The number of replicas to maintain of each block. The default value is 1.
        /// </value>
        /// <remarks>
        /// The recommended value is 3 unless you have fewer than 3 data servers.
        /// </remarks>
        [ConfigurationProperty("replicationFactor", DefaultValue = 1, IsRequired = false, IsKey = false)]
        public int ReplicationFactor
        {
            get { return (int)this["replicationFactor"]; }
            set { this["replicationFactor"] = value; }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the server should listen on both IPv6 and IPv4.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the server should listen on both IPv6 and IPv4; <see langword="false"/>
        /// if the server should listen only on IPv6 if it's available, and otherwise on IPv4; <see langword="null"/>
        /// if this should be decided based on the king of operating system. The default value is <see langword="null"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   On Linux, if a socket binds to an IPv6 port it automatically also binds to an associated IPv4 port. Therefore,
        ///   this value should be <see langword="false"/> (an exception will be thrown if it's not).
        /// </para>
        /// <para>
        ///   If this property is unspecified, it will default to <see langword="true"/> on Windows and <see langword="false"/> on Unix
        ///   (which is correct for Linux, but may not be appropriate for other Unix operating systems).
        /// </para>
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1704:IdentifiersShouldBeSpelledCorrectly", MessageId = "Pv"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Pv"),
        ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = null, IsRequired = false, IsKey = false)]
        public bool? ListenIPv4AndIPv6
        {
            get { return (bool?)this["listenIPv4AndIPv6"]; }
            set { this["listenIPv4AndIPv6"] = value; }
        }

        /// <summary>
        /// Gets or sets the directory in which the file system image and edit log are stored.
        /// </summary>
        /// <value>
        /// The directory in which the file system image and edit log are stored.
        /// </value>
        [ConfigurationProperty("imageDirectory", IsRequired = true, IsKey = false)]
        public string ImageDirectory
        {
            get { return (string)this["imageDirectory"]; }
            set { this["imageDirectory"] = value; }
        }

        /// <summary>
        /// Gets or sets the minimum amount of time that a data server must be unresponsive before
        /// it is considered dead.
        /// </summary>
        /// <value>
        /// The minimum amount of time, in seconds, that a data server must be unresponsive before
        /// it is considered dead. The default value is 300.
        /// </value>
        /// <remarks>
        /// Depending on circumstances, it can be up to twice as long before a data server is actually considered dead.
        /// </remarks>
        [ConfigurationProperty("dataServerTimeout", DefaultValue = 300, IsRequired = false, IsKey = false)]
        public int DataServerTimeout
        {
            get { return (int)this["dataServerTimeout"]; }
            set { this["dataServerTimeout"] = value; }
        }

        /// <summary>
        /// Gets or sets the minimum amount of space that a data server must have available in order to be eligible
        /// for new blocks.
        /// </summary>
        /// <value>
        /// The minimum amount of space that a data server must have available in order to be eligible
        /// for new blocks. The default value is 1 gigabyte.
        /// </value>
        [ConfigurationProperty("dataServerFreeSpaceThreshold", DefaultValue = "1GB", IsRequired = false, IsKey = false)]
        public BinarySize DataServerFreeSpaceThreshold
        {
            get { return (BinarySize)this["dataServerFreeSpaceThreshold"]; }
            set { this["dataServerFreeSpaceThreshold"] = value; }
        }
    }
}
