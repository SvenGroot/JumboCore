// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Represents a reference to a <see cref="Type"/> that will be serialized to XML using the type name.
    /// </summary>
    public struct TypeReference : IXmlSerializable, IEquatable<TypeReference>
    {
        private static bool _resolveTypes = true;
        private string _typeName;
        private Type _type;

        /// <summary>
        /// A <see cref="TypeReference" /> instance that doesn't reference any type.
        /// </summary>
        public static readonly TypeReference Empty = new TypeReference();

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeReference"/> structure using the specified type.
        /// </summary>
        /// <param name="type">The type this instance should reference. May be <see langword="null"/>.</param>
        public TypeReference(Type type)
        {
            _type = type;
            _typeName = type == null ? null : type.AssemblyQualifiedName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeReference"/> structure using the specified type name.
        /// </summary>
        /// <param name="typeName">Name of the type. May be <see langword="null"/>.</param>
        public TypeReference(string typeName)
        {
            _type = null;
            _typeName = typeName;
        }

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="TypeReference"/> instances should resolve the type specified by <see cref="TypeName"/>
        /// if <see cref="ReferencedType"/> wasn't explicitly set.
        /// </summary>
        /// <value>
        /// 	<see langword="true"/> if types should be resolved; otherwise, <see langword="false"/>. The default value is <see langword="true"/>.
        /// </value>
        /// <remarks>
        /// <para>
        ///   The <see cref="TypeReference"/> class is used as part of the configuration for Jumbo Jet jobs. The job server will load the
        ///   configuration, but loading referenced assemblies into the job server should be avoided. This property allows you to ensure
        ///   that even if e.g. a debugger accesses the <see cref="ReferencedType"/> property the type will not be loaded.
        /// </para>
        /// </remarks>
        public static bool ResolveTypes
        {
            get { return _resolveTypes; }
            set { _resolveTypes = value; }
        }


        /// <summary>
        /// Gets or sets the type that this <see cref="TypeReference" /> references.
        /// </summary>
        /// <value>
        /// The <see cref="Type"/> that this <see cref="TypeReference"/> references.
        /// </value>
        /// <exception cref="System.InvalidOperationException">Resolving type references is disabled.</exception>
        [SuppressMessage("Design", "CA1065:Do not raise exceptions in unexpected locations", Justification = "False positive.")]
        public Type ReferencedType
        {
            get
            {
                if (_type == null && _typeName != null)
                {
                    if (ResolveTypes)
                        _type = Type.GetType(_typeName, true);
                    else
                        throw new InvalidOperationException("Resolving type references is disabled.");
                }
                return _type;
            }
        }

        /// <summary>
        /// Gets or sets the name of the type that this <see cref="TypeReference" /> references.
        /// </summary>
        /// <value>
        /// The assembly qualified name of the referenced type.
        /// </value>
        public string TypeName
        {
            get
            {
                return _typeName;
            }
        }

        /// <summary>
        /// Converts this instance to a string representation.
        /// </summary>
        /// <returns>The name of the type that this <see cref="TypeReference"/> references, or an empty string if <see cref="TypeName"/> is <see langword="null"/>.</returns>
        public override string ToString()
        {
            return TypeName ?? string.Empty;
        }


        /// <summary>
        /// Implicitly converts a <see cref="Type"/> to a <see cref="TypeReference"/>.
        /// </summary>
        /// <param name="type">The type to reference.</param>
        /// <returns>An instance of <see cref="TypeReference"/> that references <paramref name="type"/>.</returns>
        [SuppressMessage("Usage", "CA2225:Operator overloads have named alternates", Justification = "Constructor is the alternative.")]
        public static implicit operator TypeReference(Type type)
        {
            return new TypeReference(type);
        }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="TypeReference"/>.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to compare to the current <see cref="TypeReference"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current <see cref="TypeReference"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            if (obj is TypeReference)
            {
                TypeReference right = (TypeReference)obj;
                return right.ReferencedType == ReferencedType;
            }
            else
                return false;
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>A hash code for the current <see cref="Object"/>.</returns>
        public override int GetHashCode()
        {
            if (ReferencedType == null)
                return 0;
            else
                return ReferencedType.GetHashCode();
        }

        /// <summary>
        /// Determines whether two specified <see cref="TypeReference"/> object have the same value.
        /// </summary>
        /// <param name="left">A <see cref="TypeReference"/>.</param>
        /// <param name="right">A <see cref="TypeReference"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(TypeReference left, TypeReference right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Determines whether two specified <see cref="TypeReference"/> object have different values.
        /// </summary>
        /// <param name="left">A <see cref="TypeReference"/>.</param>
        /// <param name="right">A <see cref="TypeReference"/>.</param>
        /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(TypeReference left, TypeReference right)
        {
            return !object.Equals(left, right);
        }

        #region IXmlSerializable Members

        System.Xml.Schema.XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }

        void IXmlSerializable.ReadXml(System.Xml.XmlReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));
            if (reader.IsEmptyElement)
                reader.ReadStartElement();
            else
            {
                _typeName = reader.ReadString();
                _type = null;
                reader.ReadEndElement();
            }
        }

        void IXmlSerializable.WriteXml(System.Xml.XmlWriter writer)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            writer.WriteString(TypeName);
        }

        #endregion

        #region IEquatable Members

        public bool Equals([AllowNull] TypeReference other)
        {
            return other != null && ReferencedType == other.ReferencedType;
        }

        #endregion
    }
}
