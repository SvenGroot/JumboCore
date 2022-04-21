// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Provides configuration settings for the data servers.
    /// </summary>
    public class DataServerConfigurationElement : ConfigurationElement
    {
        /// <summary>
        /// Gets or sets the port number on which the data server listens for client connections.
        /// </summary>
        /// <value>
        /// The port number on which the data server listens for client connections. The default value is 9001.
        /// </value>
        [ConfigurationProperty("port", DefaultValue = 9001, IsRequired = false, IsKey = false)]
        public int Port
        {
            get { return (int)this["port"]; }
            set { this["port"] = value; }
        }

        /// <summary>
        /// Gets or sets the path to the directory where the data server stores block files.
        /// </summary>
        /// <value>
        /// The path to the directory where the data server stores block files.
        /// </value>
        [ConfigurationProperty("blockStorageDirectory", IsRequired = true, IsKey = false)]
        public string BlockStorageDirectory
        {
            get { return (string)this["blockStorageDirectory"]; }
            set { this["blockStorageDirectory"] = value; }
        }

        /// <summary>
        /// Gets or sets value that indicates whether the server should listen on both IPv6 and IPv4.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if the server should listen on both IPv6 and IPv4; <see langword="false"/>
        /// if the server should listen only on IPv6 if it's available, and otherwise on IPv4.
        /// </value>
        [ConfigurationProperty("listenIPv4AndIPv6", DefaultValue = true, IsRequired = false, IsKey = false)]
        public bool ListenIPv4AndIPv6
        {
            get { return (bool)this["listenIPv4AndIPv6"]; }
            set { this["listenIPv4AndIPv6"] = value; }
        }

        /// <summary>
        /// Gets or sets the interval at which the data server should send status updates
        /// (including disk space reports) to the name server.
        /// </summary>
        /// <value>
        /// The interval at which the data server should send status updates, in seconds. The default value is 60.
        /// </value>
        /// <remarks>
        /// Disk space status updates are always sent after blocks are received or deleted, regardless
        /// of this value.
        /// </remarks>
        [ConfigurationProperty("statusUpdateInterval", DefaultValue = 60, IsRequired = false, IsKey = false)]
        public int StatusUpdateInterval
        {
            get { return (int)this["statusUpdateInterval"]; }
            set { this["statusUpdateInterval"] = value; }
        }

        /// <summary>
        /// Gets or sets the size of the write buffer for block files.
        /// </summary>
        /// <value>
        /// The size of the write buffer for block files. The default value is 128 kilobytes.
        /// </value>
        [ConfigurationProperty("writeBufferSize", DefaultValue = "128KB", IsRequired = false, IsKey = false)]
        public BinarySize WriteBufferSize
        {
            get { return (BinarySize)this["writeBufferSize"]; }
            set { this["writeBufferSize"] = value; }
        }

        /// <summary>
        /// Gets or sets the size of the read buffer for block files.
        /// </summary>
        /// <value>
        /// The size of the read buffer for block files. The default value is 128 kilobytes.
        /// </value>
        [ConfigurationProperty("readBufferSize", DefaultValue = "128KB", IsRequired = false, IsKey = false)]
        public BinarySize ReadBufferSize
        {
            get { return (BinarySize)this["readBufferSize"]; }
            set { this["readBufferSize"] = value; }
        }
    }
}
