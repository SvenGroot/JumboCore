// Copyright (c) Sven Groot (Ookii.org)
using System;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Specifies the <see cref="IRawComparer{T}"/> implementation for a type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class RawComparerAttribute : Attribute
    {
        private readonly string _rawComparerTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="RawComparerAttribute"/> class.
        /// </summary>
        /// <param name="rawComparerTypeName">The type name of the type implementing <see cref="IRawComparer{T}"/>.</param>
        public RawComparerAttribute(string rawComparerTypeName)
        {
            _rawComparerTypeName = rawComparerTypeName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RawComparerAttribute"/> class.
        /// </summary>
        /// <param name="rawComparerTypeName">The type that implements <see cref="IRawComparer{T}"/>.</param>
        public RawComparerAttribute(Type rawComparerTypeName)
        {
            ArgumentNullException.ThrowIfNull(rawComparerTypeName);
            _rawComparerTypeName = rawComparerTypeName.AssemblyQualifiedName ?? string.Empty;
        }

        /// <summary>
        /// Gets the name of the type that implements <see cref="IRawComparer{T}"/>.
        /// </summary>
        /// <value>
        /// The name of a type that implements <see cref="IRawComparer{T}"/>.
        /// </value>
        public string RawComparerTypeName
        {
            get { return _rawComparerTypeName; }
        }
    }
}
