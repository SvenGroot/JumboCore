﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Dfs
{
    /// <summary>
    /// Exception that is thrown if the name server is accessed while still in safe mode.
    /// </summary>
    [Serializable]
    public class SafeModeException : DfsException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SafeModeException"/> class. 
        /// </summary>
        public SafeModeException() { }
        /// <summary>
        /// Initializes a new instance of the <see cref="SafeModeException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public SafeModeException(string message) : base(message) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="SafeModeException"/> class with a specified error message and a reference to the inner <see cref="SafeModeException"/> that is the cause of this <see cref="SafeModeException"/>. 
        /// </summary>
        /// <param name="message">The error message that explains the reason for the <see cref="SafeModeException"/>.</param>
        /// <param name="inner">The <see cref="SafeModeException"/> that is the cause of the current <see cref="SafeModeException"/>, or a null reference (Nothing in Visual Basic) if no inner <see cref="SafeModeException"/> is specified.</param>
        public SafeModeException(string message, Exception inner) : base(message, inner) { }
        /// <summary>
        /// Initializes a new instance of the <see cref="SafeModeException"/> class with serialized data. 
        /// </summary>
        /// <param name="info">The <see cref="System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the <see cref="SafeModeException"/> being thrown.</param>
        /// <param name="context">The <see cref="System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        protected SafeModeException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
