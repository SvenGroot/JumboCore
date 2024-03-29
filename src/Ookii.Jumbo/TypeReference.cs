﻿// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Diagnostics.CodeAnalysis;
using System.Xml.Serialization;

namespace Ookii.Jumbo;

/// <summary>
/// Represents a reference to a <see cref="Type"/> that will be serialized to XML using the type name.
/// </summary>
public struct TypeReference : IXmlSerializable, IEquatable<TypeReference>
{
    private Type? _type;

    /// <summary>
    /// A <see cref="TypeReference" /> instance that doesn't reference any type.
    /// </summary>
    public static readonly TypeReference Empty = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="TypeReference"/> structure using the specified type.
    /// </summary>
    /// <param name="type">The type this instance should reference. May be <see langword="null"/>.</param>
    public TypeReference(Type? type)
    {
        _type = type;
        TypeName = type?.AssemblyQualifiedName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TypeReference"/> structure using the specified type name.
    /// </summary>
    /// <param name="typeName">Name of the type. May be <see langword="null"/>.</param>
    public TypeReference(string? typeName)
    {
        _type = null;
        TypeName = typeName;
    }

    /// <summary>
    /// Gets or sets a value indicating whether <see cref="TypeReference"/> instances should resolve
    /// the type specified by <see cref="TypeName"/> if the type isn't already known.
    /// </summary>
    /// <value>
    ///     <see langword="true"/> if types should be resolved; otherwise, <see langword="false"/>.
    ///     The default value is <see langword="true"/>.
    /// </value>
    /// <remarks>
    /// <para>
    ///   The <see cref="TypeReference"/> class is used as part of the configuration for Jumbo Jet
    ///   jobs. The job server will load the configuration, but loading referenced assemblies into
    ///   the job server should be avoided. This property allows you to ensure that even if e.g. a
    ///   debugger executes the <see cref="GetReferencedType"/> method the type will not be loaded.
    /// </para>
    /// </remarks>
    public static bool ResolveTypes { get; set; } = true;

    /// <summary>
    /// Gets type that this <see cref="TypeReference" /> references, if any.
    /// </summary>
    /// <param name="type">
    /// When this method returns <see langword="true"/>, contains the type referenced by this
    /// instance.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if this instance references a type and it was resolved;
    /// <see langword="false"/> if this instance did not reference a type (the <see cref="TypeName"/>
    /// property is <see langword="null"/>.
    /// </returns>
    /// <exception cref="System.InvalidOperationException">
    /// Resolving type references is disabled.
    /// </exception>
    /// <remarks>
    /// <para>
    ///   This method still throws an exception if the <see cref="ResolveTypes"/> property is
    ///   <see langword="false"/>, or if the type specified by the <see cref="TypeName"/> property
    ///   could not be resolved.
    /// </para>
    /// </remarks>
    public bool TryGetReferencedType([MaybeNullWhen(false)] out Type type)
    {
        if (TypeName == null)
        {
            type = null;
            return false;
        }

        if (_type == null)
        {
            if (!ResolveTypes)
            {
                // This should always throw, not return false, because it means this is called in
                // a context where it shouldn't be.
                throw new InvalidOperationException("Resolving type references is disabled.");
            }

            _type = Type.GetType(TypeName, true)!;
        }

        type = _type;
        return true;
    }

    /// <summary>
    /// Gets the type that this <see cref="TypeReference" /> references.
    /// </summary>
    /// <returns>
    /// The <see cref="Type"/> that this <see cref="TypeReference"/> references.
    /// </returns>
    /// <exception cref="System.InvalidOperationException">
    /// Resolving type references is disabled, or no type is referenced.
    /// </exception>
    public Type GetReferencedType()
    {
        if (_type != null)
        {
            return _type;
        }

        if (TypeName == null)
        {
            throw new InvalidOperationException("No type is referenced by this TypeReference object.");
        }

        if (!ResolveTypes)
        {
            throw new InvalidOperationException("Resolving type references is disabled.");
        }

        _type = Type.GetType(TypeName, true)!;
        return _type;
    }

    /// <summary>
    /// Gets or sets the name of the type that this <see cref="TypeReference" /> references.
    /// </summary>
    /// <value>
    /// The assembly qualified name of the referenced type.
    /// </value>
    public string? TypeName { get; private set; }

    /// <summary>
    /// Converts this instance to a string representation.
    /// </summary>
    /// <returns>The name of the type that this <see cref="TypeReference"/> references, or an empty string if <see cref="TypeName"/> is <see langword="null"/>.</returns>
    public override readonly string ToString() => TypeName ?? string.Empty;

    /// <summary>
    /// Implicitly converts a <see cref="Type"/> to a <see cref="TypeReference"/>.
    /// </summary>
    /// <param name="type">The type to reference.</param>
    /// <returns>An instance of <see cref="TypeReference"/> that references <paramref name="type"/>.</returns>
    public static implicit operator TypeReference(Type type) => new(type);

    /// <summary>
    /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="TypeReference"/>.
    /// </summary>
    /// <param name="obj">The <see cref="Object"/> to compare to the current <see cref="TypeReference"/>.</param>
    /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current <see cref="TypeReference"/>; otherwise, <see langword="false"/>.</returns>
    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        if (obj is TypeReference right)
        {
            return right.TypeName == TypeName;
        }

        return false;
    }

    /// <summary>
    /// Serves as a hash function for a particular type. 
    /// </summary>
    /// <returns>A hash code for the current <see cref="Object"/>.</returns>
    public override readonly int GetHashCode() => TypeName?.GetHashCode() ?? 0;

    /// <summary>
    /// Determines whether two specified <see cref="TypeReference"/> object have the same value.
    /// </summary>
    /// <param name="left">A <see cref="TypeReference"/>.</param>
    /// <param name="right">A <see cref="TypeReference"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator ==(TypeReference left, TypeReference right) => left.Equals(right);

    /// <summary>
    /// Determines whether two specified <see cref="TypeReference"/> object have different values.
    /// </summary>
    /// <param name="left">A <see cref="TypeReference"/>.</param>
    /// <param name="right">A <see cref="TypeReference"/>.</param>
    /// <returns><see langword="true"/> if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(TypeReference left, TypeReference right) => !left.Equals(right);

    #region IXmlSerializable Members

    readonly System.Xml.Schema.XmlSchema? IXmlSerializable.GetSchema() => null;

    void IXmlSerializable.ReadXml(System.Xml.XmlReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        if (reader.IsEmptyElement)
        {
            reader.ReadStartElement();
        }
        else
        {
            TypeName = reader.ReadString();
            _type = null;
            reader.ReadEndElement();
        }
    }

    readonly void IXmlSerializable.WriteXml(System.Xml.XmlWriter writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteString(TypeName);
    }

    #endregion

    #region IEquatable Members

    /// <summary>
    /// Returns a value indicating whether the this instance is equal to the specified instance.
    /// </summary>
    /// <param name="other">The instance to compare to.</param>
    /// <returns><see langword="true" /> if the instances are equal; otherwise, <see langword="false" />.</returns>
    public readonly bool Equals(TypeReference other) => TypeName == other.TypeName;

    #endregion
}
