// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Represents a rack that groups together nodes in the network topology.
    /// </summary>
    public class Rack
    {
        #region Nested types

        private class NodeCollection : ExtendedCollection<TopologyNode>
        {
            private Rack _rack;

            public NodeCollection(Rack rack)
            {
                _rack = rack;
            }

            protected override void InsertItem(int index, TopologyNode item)
            {
                if (item == null)
                    throw new ArgumentNullException(nameof(item));
                if (item.Rack != null)
                    throw new ArgumentException("The specified node is already part of another rack.");
                base.InsertItem(index, item);
                item.Rack = _rack;
            }

            protected override void SetItem(int index, TopologyNode item)
            {
                if (item == null)
                    throw new ArgumentNullException(nameof(item));
                if (item.Rack != null)
                    throw new ArgumentException("The specified node is already part of another rack.");
                this[index].Rack = null;
                base.SetItem(index, item);
                item.Rack = _rack;
            }

            protected override void RemoveItem(int index)
            {
                this[index].Rack = null;
                base.RemoveItem(index);
            }

            protected override void ClearItems()
            {
                foreach (TopologyNode node in this)
                    node.Rack = null;
                base.ClearItems();
            }
        }

        #endregion

        private readonly NodeCollection _nodes;

        /// <summary>
        /// Initializes a new instance of the <see cref="Rack"/> class.
        /// </summary>
        /// <param name="rackId">The unique identifier for the rack.</param>
        public Rack(string rackId)
        {
            if (rackId == null)
                throw new ArgumentNullException(nameof(rackId));

            _nodes = new NodeCollection(this);
            RackId = rackId;
        }

        /// <summary>
        /// Gets the unique identifier for the rack.
        /// </summary>
        public string RackId { get; private set; }

        /// <summary>
        /// Gets the nodes that are part of this rack.
        /// </summary>
        public Collection<TopologyNode> Nodes
        {
            get { return _nodes; }
        }
    }
}
