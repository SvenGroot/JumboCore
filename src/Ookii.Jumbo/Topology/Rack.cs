// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.ObjectModel;

namespace Ookii.Jumbo.Topology;

/// <summary>
/// Represents a rack that groups together nodes in the network topology.
/// </summary>
public class Rack
{
    #region Nested types

    private class NodeCollection : ExtendedCollection<TopologyNode>
    {
        private readonly Rack _rack;

        public NodeCollection(Rack rack)
        {
            _rack = rack;
        }

        protected override void InsertItem(int index, TopologyNode item)
        {
            ArgumentNullException.ThrowIfNull(item);
            if (item.Rack != null)
            {
                throw new ArgumentException("The specified node is already part of another rack.");
            }

            base.InsertItem(index, item);
            item.Rack = _rack;
        }

        protected override void SetItem(int index, TopologyNode item)
        {
            ArgumentNullException.ThrowIfNull(item);
            if (item.Rack != null)
            {
                throw new ArgumentException("The specified node is already part of another rack.");
            }

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
            foreach (var node in this)
            {
                node.Rack = null;
            }

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
        ArgumentNullException.ThrowIfNull(rackId);

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
