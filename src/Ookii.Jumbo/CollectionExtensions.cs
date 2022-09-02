// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo
{
    /// <summary>
    /// Provides extension methods for <see cref="Collection{T}"/>.
    /// </summary>
    public static class CollectionExtensions
    {
        /// <summary>
        /// Adds a range of elements to the collection.
        /// </summary>
        /// <typeparam name="T">The type of the elements.</typeparam>
        /// <param name="target">The collection to add the elements to.</param>
        /// <param name="collection">The collection containing the elements to add.</param>
        public static void AddRange<T>(this Collection<T> target, IEnumerable<T> collection)
        {
            ArgumentNullException.ThrowIfNull(target);
            ArgumentNullException.ThrowIfNull(collection);

            var extendedCollection = target as ExtendedCollection<T>;
            if (extendedCollection != null)
                extendedCollection.AddRange(collection);
            else
            {
                foreach (var item in collection)
                    target.Add(item);
            }
        }
    }
}
