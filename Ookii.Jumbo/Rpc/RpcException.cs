// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Rpc
{
    /// <summary>
    /// Indicates an error occurred with a remoting operation.
    /// </summary>
    [Serializable]
    public class RpcException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RpcException"/> class. 
        /// </summary>
        public RpcException() { }
        /// <summary>
        /// Initializes a new instance of the <see cref="RpcException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public RpcException(string message) : base(message) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="RpcException"/> class with a specified error message and a reference to the inner <see cref="RpcException"/> that is the cause of this <see cref="RpcException"/>. 
        /// </summary>
        /// <param name="message">The error message that explains the reason for the <see cref="RpcException"/>.</param>
        /// <param name="inner">The <see cref="RpcException"/> that is the cause of the current <see cref="RpcException"/>, or a <see langword="null"/> if no inner <see cref="RpcException"/> is specified.</param>
        public RpcException(string message, Exception inner) : base(message, inner) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="RpcException"/> class with serialized data. 
        /// </summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the <see cref="RpcException"/> being thrown.</param>
        /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected RpcException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
