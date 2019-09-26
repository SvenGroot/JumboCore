// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.IO;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// Abstract base class for classes implementing <see cref="IWritable"/> that encapsulate a simple existing comparable type.
    /// </summary>
    /// <typeparam name="T">The underlying type of the writable.</typeparam>
    public abstract class WritableComparable<T> : IWritable, IComparable<WritableComparable<T>>, IComparable, IEquatable<WritableComparable<T>>
    {
        /// <summary>
        /// Gets or sets the underlying string value.
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// Gets a comparer to use to compare the values.
        /// </summary>
        protected virtual IComparer<T> Comparer
        {
            get
            {
                return Comparer<T>.Default;
            }
        }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="WritableComparable{T}"/>.
        /// </summary>
        /// <param name="obj">The <see cref="Object"/> to compare with the current <see cref="WritableComparable{T}"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="Object"/> is equal to the current 
        /// <see cref="WritableComparable{T}"/>; otherwise, <see langword="false"/>.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as WritableComparable<T>);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>A hash code for the current <see cref="WritableComparable{T}"/>.</returns>
        public override int GetHashCode()
        {
            return Value == null ? 0 : Value.GetHashCode();
        }

        /// <summary>
        /// Returns a string representation of this <see cref="WritableComparable{T}"/>.
        /// </summary>
        /// <returns>A string representation of this <see cref="WritableComparable{T}"/>.</returns>
        public override string ToString()
        {
            return Value.ToString();
        }

        /// <summary>
        /// Compares two instances of <see cref="WritableComparable{T}"/> for equality.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if the objects are equal; otherwise, <see langword="false"/>.</returns>
        public static bool operator ==(WritableComparable<T> left, WritableComparable<T> right)
        {
            return object.Equals(left, right);
        }

        /// <summary>
        /// Compares two instances of <see cref="WritableComparable{T}"/> for inequality.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if the objects are not equal; otherwise, <see langword="false"/>.</returns>
        public static bool operator !=(WritableComparable<T> left, WritableComparable<T> right)
        {
            return !object.Equals(left, right);
        }

        /// <summary>
        /// Checks if the first operand sorts before the right operand.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> comes earlier in
        /// the sort order than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <(WritableComparable<T> left, WritableComparable<T> right)
        {
            if( left == null )
            {
                if( right == null )
                    return false;
                else
                    return true;
            }
            else
                return left.CompareTo(right) < 0;
        }

        /// <summary>
        /// Checks if the first operand sorts before or equal to the right operand.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> comes earlier in
        /// the sort order than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator <=(WritableComparable<T> left, WritableComparable<T> right)
        {
            if (left == null)
            {
                return true;
            }
            else
                return left.CompareTo(right) <= 0;
        }

        /// <summary>
        /// Checks if the first operand sorts after the right operand.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> comes later in
        /// the sort order than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >(WritableComparable<T> left, WritableComparable<T> right)
        {
            if( left == null )
            {
                return false;
            }
            else
                return left.CompareTo(right) > 0;
        }

        /// <summary>
        /// Checks if the first operand sorts after or equal to the right operand.
        /// </summary>
        /// <param name="left">The left operand of the comparison.</param>
        /// <param name="right">The right operand of the comparison.</param>
        /// <returns><see langword="true"/> if <paramref name="left"/> comes later in
        /// the sort order than <paramref name="right"/>; otherwise, <see langword="false"/>.</returns>
        public static bool operator >=(WritableComparable<T> left, WritableComparable<T> right)
        {
            if (left == null)
            {
                return right == null;
            }
            else
                return left.CompareTo(right) >= 0;
        }

        #region IWritable Members

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public abstract void Write(System.IO.BinaryWriter writer);

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public abstract void Read(System.IO.BinaryReader reader);

        #endregion

        #region IComparable<WritableComparable<T>> Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="other">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(WritableComparable<T> other)
        {
            if( other == null )
                return 1;

            return Comparer.Compare(Value, other.Value);
        }

        #endregion

        #region IComparable Members

        /// <summary>
        /// Compares the current instance with another object of the same type and returns an integer that 
        /// indicates whether the current instance precedes, follows, or occurs in the same position in the 
        /// sort order as the other object. 
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>A 32-bit signed integer that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(object obj)
        {
            return CompareTo(obj as WritableComparable<T>);
        }

        #endregion

        #region IEquatable<WritableComparable<T>> Members

        /// <summary>
        /// Determines whether the specified <see cref="WritableComparable{T}"/> is equal to the current <see cref="WritableComparable{T}"/>.
        /// </summary>
        /// <param name="other">The <see cref="WritableComparable{T}"/> to compare with the current <see cref="WritableComparable{T}"/>.</param>
        /// <returns><see langword="true"/> if the specified <see cref="WritableComparable{T}"/> is equal to the current 
        /// <see cref="WritableComparable{T}"/>; otherwise, <see langword="false"/>.</returns>
        public virtual bool Equals(WritableComparable<T> other)
        {
            if( other == null )
                return false;
            return object.Equals(Value, other.Value);
        }

        #endregion
    }
}
