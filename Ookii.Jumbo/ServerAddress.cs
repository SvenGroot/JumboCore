// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Represents the host name and port number of a data server or task server.
    /// </summary>
    [Serializable]
    public class ServerAddress : IComparable<ServerAddress>
    {
        private readonly string _hostName;
        private readonly int _port;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerAddress"/> class.
        /// </summary>
        /// <remarks>
        /// This constructor is used for deserialization purposes only.
        /// </remarks>
        public ServerAddress()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerAddress"/> class with the specified host name and port number.
        /// </summary>
        /// <param name="hostName">The host name of the server.</param>
        /// <param name="port">The port number of the server.</param>
        public ServerAddress(string hostName, int port)
        {
            if (hostName == null)
                throw new ArgumentNullException(nameof(hostName));
            if (port <= 0 || port > 0xFFFF)
                throw new ArgumentOutOfRangeException(nameof(port));
            _hostName = hostName;
            _port = port;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerAddress"/> class with the specified address.
        /// </summary>
        /// <param name="address">A string representation of the server address, in the form hostname:port, e.g. "my_server:9000".</param>
        public ServerAddress(string address)
        {
            if (address == null)
                throw new ArgumentNullException(nameof(address));
            string[] parts = address.Split(':');
            if (parts.Length != 2)
                throw new ArgumentException("Invalid server address string.", nameof(address));

            _hostName = parts[0];
            _port = Convert.ToInt32(parts[1], System.Globalization.CultureInfo.InvariantCulture);

            if (Port <= 0 || Port > 0xFFFF)
                throw new ArgumentOutOfRangeException(nameof(address), "Invalid port number in server address string");
        }

        /// <summary>
        /// Gets the host name of the server.
        /// </summary>
        /// <value>
        /// The host name of the server.
        /// </value>
        public string HostName
        {
            get { return _hostName; }
        }
        /// <summary>
        /// Gets the port number of the server.
        /// </summary>
        /// <value>
        /// The port number of the server.
        /// </value>
        public int Port
        {
            get { return _port; }
        }

        /// <summary>
        /// Returns a string representation of the current <see cref="ServerAddress"/>.
        /// </summary>
        /// <returns>A string representation of the current <see cref="ServerAddress"/>.</returns>
        public override string ToString()
        {
            return string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}:{1}", HostName, Port);
        }

        /// <summary>
        /// Compares this <see cref="ServerAddress"/> to another object.
        /// </summary>
        /// <param name="obj">The object to compare to.</param>
        /// <returns><see langword="true"/> if this <see cref="ServerAddress"/> is equal to the specified object; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            ServerAddress other = obj as ServerAddress;
            return other == null ? false : (HostName == other.HostName && Port == other.Port);
        }

        /// <summary>
        /// Gets a hash code that identifies this object.
        /// </summary>
        /// <returns>A hash code that identifies this object.</returns>
        public override int GetHashCode()
        {
            return HostName.GetHashCode(StringComparison.Ordinal) ^ Port;
        }

        /// <summary>
        /// Determines whether two specified <see cref="ServerAddress"/> objects have the same value.
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is equal to <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(ServerAddress left, ServerAddress right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Determines whether two specified <see cref="ServerAddress"/> objects have different values.
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(ServerAddress left, ServerAddress right)
        {
            return !object.Equals(left, right);
        }

        /// <summary>
        /// Determines whether one specified <see cref="ServerAddress"/> is less than another specified <see cref="ServerAddress"/>
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(ServerAddress left, ServerAddress right)
        {
            return Comparer<ServerAddress>.Default.Compare(left, right) < 0;
        }

        /// <summary>
        /// Determines whether one specified <see cref="ServerAddress"/> is less than or equal to another specified <see cref="ServerAddress"/>
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is less than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <=(ServerAddress left, ServerAddress right)
        {
            return Comparer<ServerAddress>.Default.Compare(left, right) <= 0;
        }

        /// <summary>
        /// Determines whether one specified <see cref="ServerAddress"/> is greater than another specified <see cref="ServerAddress"/>
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(ServerAddress left, ServerAddress right)
        {
            return Comparer<ServerAddress>.Default.Compare(left, right) > 0;
        }


        /// <summary>
        /// Determines whether one specified <see cref="ServerAddress"/> is greater than or equal to another specified <see cref="ServerAddress"/>
        /// </summary>
        /// <param name="left">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <param name="right">A <see cref="ServerAddress"/> or <see langword="null"/>.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> is greater than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >=(ServerAddress left, ServerAddress right)
        {
            return Comparer<ServerAddress>.Default.Compare(left, right) >= 0;
        }

        #region IComparable<ServerAddress> Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="other">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(ServerAddress other)
        {
            if (other == null)
                return 1;
            int result = StringComparer.Ordinal.Compare(HostName, other.HostName);
            if (result == 0)
                result = Port - other.Port;
            return result;
        }

        #endregion
    }
}
