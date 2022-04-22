// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

namespace Ookii.Jumbo.IO
{
    /// <summary>
    /// A collection of <see cref="IWritable"/> items which itself also implements <see cref="IWritable"/>.
    /// </summary>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <remarks>
    /// <para>
    ///   You cannot add <see langword="null"/> as an item in this collection.
    /// </para>
    /// </remarks>
    public sealed class WritableCollection<T> : IList<T>, IWritable, IList
        where T : IWritable
    {
        private List<T> _items;

        /// <summary>
        /// Initializes a new instance of the <see cref="WritableCollection&lt;T&gt;"/> class that is empty and has the default initial capacity.
        /// </summary>
        public WritableCollection()
        {
            _items = new List<T>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="WritableCollection&lt;T&gt;"/> class that 
        /// is empty and has the specified initial capacity. 
        /// </summary>
        /// <param name="capacity">The number of elements that the new list can initially store.</param>
        public WritableCollection(int capacity)
        {
            _items = new List<T>(capacity);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="WritableCollection&lt;T&gt;"/> class that 
        /// contains elements copied from the specified collection and has sufficient capacity to accommodate the number of elements copied. 
        /// </summary>
        /// <param name="collection">The collection whose elements are copied to the new list.</param>
        public WritableCollection(IEnumerable<T> collection)
        {
            _items = new List<T>(collection);
        }

        /// <summary>
        /// Searches for the specified object and returns the zero-based index of the first occurrence
        /// within the range of elements in the <see cref="WritableCollection{T}"/> that extends from the specified index to the last element.
        /// </summary>
        /// <param name="item">The object to locate in the <see cref="WritableCollection{T}"/>. The value can be <see langword="null"/> for reference types.</param>
        /// <returns>The zero-based index of the first occurrence of <paramref name="item"/> within the entire <see cref="WritableCollection{T}"/>, if found; otherwise, –1.</returns>
        public int IndexOf(T item)
        {
            return _items.IndexOf(item);
        }

        /// <summary>
        /// Inserts an element into the <see cref="WritableCollection{T}"/> at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index at which <paramref name="item"/> should be inserted.</param>
        /// <param name="item">The object to insert.</param>
        public void Insert(int index, T item)
        {
            ArgumentNullException.ThrowIfNull(item);
            _items.Insert(index, item);
        }

        /// <summary>
        /// Removes the element at the specified index of the <see cref="WritableCollection{T}"/>. 
        /// </summary>
        /// <param name="index">The zero-based index of the element to remove.</param>
        public void RemoveAt(int index)
        {
            _items.RemoveAt(index);
        }

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        /// <param name="index">The zero-based index of the element to get or set.</param>
        /// <value>The element at the specified index.</value>
        public T this[int index]
        {
            get
            {
                return _items[index];
            }
            set
            {
                ArgumentNullException.ThrowIfNull(value);
                _items[index] = value;
            }
        }

        /// <summary>
        /// Adds an object to the end of the <see cref="WritableCollection{T}"/>. 
        /// </summary>
        /// <param name="item">The object to be added to the end of the <see cref="WritableCollection{T}"/>.</param>
        public void Add(T item)
        {
            ArgumentNullException.ThrowIfNull(item);
            _items.Add(item);
        }

        /// <summary>
        /// Adds the elements of the specified collection to the end of the <see cref="WritableCollection{T}"/>. 
        /// </summary>
        /// <param name="collection">The collection whose elements should be added to the end of the <see cref="WritableCollection{T}"/>.</param>
        public void AddRange(IEnumerable<T> collection)
        {
            _items.AddRange(collection);
        }

        /// <summary>
        /// Removes all elements from the <see cref="WritableCollection{T}"/>.
        /// </summary>
        public void Clear()
        {
            _items.Clear();
        }

        /// <summary>
        /// Determines whether an element is in the <see cref="WritableCollection{T}"/>. 
        /// </summary>
        /// <param name="item">The object to locate in the <see cref="WritableCollection{T}"/>. The value can be <see langword="null"/> for reference types.</param>
        /// <returns>
        /// 	<see langword="true"/> if <paramref name="item"/> is found in the <see cref="WritableCollection{T}"/>; otherwise, <see langword="false"/>.
        /// </returns>
        public bool Contains(T item)
        {
            return _items.Contains(item);
        }

        /// <summary>
        /// Copies the entire <see cref="WritableCollection{T}"/> to a compatible one-dimensional array, starting at the specified index of the target array.
        /// </summary>
        /// <param name="array">The one-dimensional <see cref="Array"/> that is the destination of the elements copied from
        /// <see cref="WritableCollection{T}"/>. The <see cref="Array"/> must have zero-based indexing.</param>
        /// <param name="arrayIndex">The zero-based index in <paramref name="array"/> at which copying begins.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            _items.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Gets the number of elements actually contained in the <see cref="WritableCollection{T}"/>.
        /// </summary>
        /// <value>The number of elements actually contained in the <see cref="WritableCollection{T}"/>.</value>
        public int Count
        {
            get { return _items.Count; }
        }

        /// <summary>
        /// Removes the first occurrence of a specific object from the <see cref="WritableCollection{T}"/>. 
        /// </summary>
        /// <param name="item">The object to remove from the <see cref="WritableCollection{T}"/>. The value can be <see langword="null"/> for reference types.</param>
        /// <returns><see langword="true"/> if <paramref name="item"/> is successfully removed; otherwise, <see langword="false"/>. 
        /// This method also returns <see langword="false"/> if <paramref name="item"/> was not found in the <see cref="WritableCollection{T}"/>.</returns>
        public bool Remove(T item)
        {
            return _items.Remove(item);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the <see cref="WritableCollection{T}"/>.
        /// </summary>
        /// <returns>An enumerator that iterates through the <see cref="WritableCollection{T}"/>.</returns>
        public IEnumerator<T> GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        /// <summary>
        /// Writes the object to the specified writer.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to serialize the object to.</param>
        public void Write(BinaryWriter writer)
        {
            WritableUtility.Write7BitEncodedInt32(writer, _items.Count);
            foreach (var item in _items)
                item.Write(writer);
        }

        /// <summary>
        /// Reads the object from the specified reader.
        /// </summary>
        /// <param name="reader">The <see cref="BinaryReader"/> to deserialize the object from.</param>
        public void Read(System.IO.BinaryReader reader)
        {
            var count = WritableUtility.Read7BitEncodedInt32(reader);

            if (_items == null)
                _items = new List<T>(count);
            else
                _items.Clear();

            for (var x = 0; x < count; ++x)
            {
                var item = (T)FormatterServices.GetUninitializedObject(typeof(T));
                item.Read(reader);
                _items.Add(item);
            }
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return "{ " + _items.ToDelimitedString() + " }";
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        /// <remarks>
        /// The hash code is computed using all the elements in the sequence.
        /// </remarks>
        public override int GetHashCode()
        {
            return _items.GetSequenceHashCode();
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object"/> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object"/> to compare with this instance.</param>
        /// <returns>
        /// 	<see langword="true"/> if the specified <see cref="System.Object"/> is equal to this instance; otherwise, <see langword="false"/>.
        /// </returns>
        /// <exception cref="System.NullReferenceException">
        /// The <paramref name="obj"/> parameter is null.
        /// </exception>
        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            if (obj == this)
                return true;

            var other = obj as WritableCollection<T>;
            if (other == null)
                return false;
            else
                return _items.SequenceEqual(other);
        }

        bool ICollection<T>.IsReadOnly
        {
            get { return ((IList<T>)_items).IsReadOnly; }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_items).GetEnumerator();
        }

        int IList.Add(object value)
        {
            return ((IList)_items).Add(value);
        }

        void IList.Clear()
        {
            ((IList)_items).Clear();
        }

        bool IList.Contains(object value)
        {
            return ((IList)_items).Contains(value);
        }

        int IList.IndexOf(object value)
        {
            return ((IList)_items).IndexOf(value);
        }

        void IList.Insert(int index, object value)
        {
            ((IList)_items).Insert(index, value);
        }

        bool IList.IsFixedSize
        {
            get { return ((IList)_items).IsFixedSize; }
        }

        bool IList.IsReadOnly
        {
            get { return ((IList)_items).IsReadOnly; }
        }

        void IList.Remove(object value)
        {
            ((IList)_items).Remove(value);
        }

        void IList.RemoveAt(int index)
        {
            ((IList)_items).RemoveAt(index);
        }

        object IList.this[int index]
        {
            get
            {
                return ((IList)_items)[index];
            }
            set
            {
                ((IList)_items)[index] = value;
            }
        }

        bool ICollection.IsSynchronized
        {
            get { return ((IList)_items).IsSynchronized; }
        }

        object ICollection.SyncRoot
        {
            get { return ((IList)_items).SyncRoot; }
        }

        void ICollection.CopyTo(Array array, int index)
        {
            ((ICollection)_items).CopyTo(array, index);
        }
    }
}
