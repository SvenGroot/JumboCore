// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Specifies the accepted input types of a class deriving from <see cref="MultiInputRecordReader{T}"/>.
    /// </summary>
    /// <remarks>
    /// If this attribute is not specified, the accepted inputs are assumed to be T.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
    public sealed class InputTypeAttribute : Attribute
    {
        private readonly Type _acceptedType;

        /// <summary>
        /// Initializes a new instance of the <see cref="InputTypeAttribute"/> class.
        /// </summary>
        /// <param name="acceptedType">The type accepted as input.</param>
        public InputTypeAttribute(Type acceptedType)
        {
            if (acceptedType == null)
                throw new ArgumentNullException(nameof(acceptedType));

            _acceptedType = acceptedType;
        }

        /// <summary>
        /// Gets the accepted input type.
        /// </summary>
        public Type AcceptedType
        {
            get { return _acceptedType; }
        }
    }
}
