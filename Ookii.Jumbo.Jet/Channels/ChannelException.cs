using System;

namespace Ookii.Jumbo.Jet.Channels
{
    /// <summary>
    /// Exception that is thrown if an error occurs in the TCP or file channel.
    /// </summary>
    [Serializable]
    public class ChannelException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelException"/> class.
        /// </summary>
        public ChannelException() { }
        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ChannelException(string message) : base(message) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner exception.</param>
        public ChannelException(string message, Exception inner) : base(message, inner) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="ChannelException"/> class.
        /// </summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        /// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
        /// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
        protected ChannelException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
