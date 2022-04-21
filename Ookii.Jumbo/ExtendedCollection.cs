// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides additional functionality for <see cref="Collection{T}" />.
    /// </summary>
    /// <typeparam name="T">The type of the items in the collection</typeparam>
    [Serializable]
    public class ExtendedCollection<T> : Collection<T>
    {
        private readonly List<T> _itemsList;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExtendedCollection{T}"/> class.
        /// </summary>
        public ExtendedCollection()
            : base(new List<T>())
        {
            _itemsList = (List<T>)Items;
        }

        /// <summary>
        /// Adds a range of elements to the collection.
        /// </summary>
        /// <param name="collection">The collection containing the elements to add.</param>
        public void AddRange(IEnumerable<T> collection)
        {
            _itemsList.AddRange(collection);
        }
    }
}
