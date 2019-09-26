// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Represents a collection of <see cref="RackConfigurationElement"/> objects in a configuration file.
    /// </summary>
    [ConfigurationCollection(typeof(RackConfigurationElement), AddItemName = "rack")]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1010:Collections should implement generic interface", Justification = "Base class is non-generic collection.")]
    public class RackConfigurationElementCollection : ConfigurationElementCollection
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RackConfigurationElementCollection"/> class.
        /// </summary>
        public RackConfigurationElementCollection()
        {
            AddElementName = "rack";
        }

        /// <summary>
        /// Gets the element in the collection at the specified index.
        /// </summary>
        /// <param name="index">The index of the item to return.</param>
        /// <returns>The item at the specified index.</returns>
        public RackConfigurationElement this[int index]
        {
            get { return (RackConfigurationElement)BaseGet(index); }
        }

        /// <summary>
        /// Creates a new element.
        /// </summary>
        /// <returns>A new <see cref="RackConfigurationElement"/>.</returns>
        protected override ConfigurationElement CreateNewElement()
        {
            return new RackConfigurationElement();
        }

        /// <summary>
        /// Gets the element key.
        /// </summary>
        /// <param name="element">The element whose key to get.</param>
        /// <returns>The <see cref="RackConfigurationElement.RackId"/> property value.</returns>
        protected override object GetElementKey(ConfigurationElement element)
        {
            if (element == null)
                throw new ArgumentNullException(nameof(element));

            return ((RackConfigurationElement)element).RackId;
        }

        /// <summary>
        /// Adds an element to the collection.
        /// </summary>
        /// <param name="element">The element to add to the collection.</param>
        public void Add(RackConfigurationElement element)
        {
            BaseAdd(element);
        }

        /// <summary>
        /// Removes an element from the collection.
        /// </summary>
        /// <param name="element">The element to remove from the collection.</param>
        public void Remove(RackConfigurationElement element)
        {
            BaseRemove(element);
        }

        /// <summary>
        /// Removes the element at the specified index from the collection.
        /// </summary>
        /// <param name="index">The index of the element to remove.</param>
        public void RemoveAt(int index)
        {
            BaseRemoveAt(index);
        }
    }
}
