// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Represents an error with the distributed file system.
    /// </summary>
    [Serializable]
    public class DfsException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DfsException"/> class. 
        /// </summary>
        public DfsException() { }
        /// <summary>
        /// Initializes a new instance of the <see cref="DfsException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public DfsException(string message) : base(message) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="DfsException"/> class with a specified error message and a reference to the inner <see cref="DfsException"/> that is the cause of this <see cref="DfsException"/>. 
        /// </summary>
        /// <param name="message">The error message that explains the reason for the <see cref="DfsException"/>.</param>
        /// <param name="inner">The <see cref="DfsException"/> that is the cause of the current <see cref="DfsException"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="DfsException"/> is specified.</param>
        public DfsException(string message, Exception inner) : base(message, inner) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="DfsException"/> class with serialized data. 
        /// </summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the <see cref="DfsException"/> being thrown.</param>
        /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected DfsException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
