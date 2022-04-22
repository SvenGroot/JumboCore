// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    // This class stores the node list and node children lists in unmanaged arrays, allocated with Marshal.AllocHGlobal.
    // The reason it does this is because the Mono GC (in Mono 2.6) doesn't compact and doesn't return memory to the OS,
    // so the frequent calls to Array.Resize cause a large amount of memory to be wasted. Using unsafe code here gives us
    // 50%+ memory savings in many cases.
    unsafe sealed class FPTree : IDisposable
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(typeof(FPTree));

        #region Nested types

        private struct HeaderTableItem
        {
            public int FirstNode { get; set; }
            public int Support { get; set; }
        }

        #endregion

        private readonly HeaderTableItem[] _headerTable;
        private readonly int _minSupport;
        private FPTreeNode* _nodes;
        private int _nodesLength;
        private NodeChildList* _nodeChildren; // Children stored separately to reduce the size of FPTreeNode.
        private int _nodeCount = 1;
        private const int _rootNode = 0;
        private const float _growthRate = 1.5f;
        private const int _minSize = 8;
        private int _weight;
        private int _mineUntilItem;
        private readonly TaskContext _config;
        private bool _disposed;

        public event EventHandler ProgressChanged;

        public FPTree(IEnumerable<ITransaction> transactions, int minSupport, int itemCount, TaskContext config)
        {
            // The transactions passed to this constructor must already be mapped and sorted by frequent item list order.
            // The itemCount indicates how many of the items from the frequent item list are in the subdatabase.
            // The highest item ID in the subdatabase should be itemCount - 1.
            int size = Math.Max(itemCount, _minSize);
            _nodes = (FPTreeNode*)Marshal.AllocHGlobal(size * sizeof(FPTreeNode));
            _nodes[_rootNode] = new FPTreeNode();
            _nodes[_rootNode].Id = -1;
            _nodesLength = size;
            _nodeChildren = (NodeChildList*)Marshal.AllocHGlobal(size * sizeof(NodeChildList));
            _nodeChildren[_rootNode] = new NodeChildList();
            _headerTable = new HeaderTableItem[itemCount];
            BuildTree(transactions);
            CleanupChildren(); // Children are not needed after tree construction.
            _log.DebugFormat("Length: {0}; Capacity: {1}; Memory: {2}", _nodeCount, _nodesLength, Process.GetCurrentProcess().PrivateMemorySize64);
            _minSupport = minSupport;
            _nodes[_rootNode].Id = -1;
            _config = config;
        }

        private FPTree(int size, int headerSize, int minSupport)
        {
            // This is used only for conditional trees, so we don't need to create the _nodeChildren array.
            _nodes = (FPTreeNode*)Marshal.AllocHGlobal(size * sizeof(FPTreeNode));
            _nodesLength = size;
            _headerTable = new HeaderTableItem[headerSize];
            _minSupport = minSupport;
            _nodes[_rootNode] = new FPTreeNode();
            _nodes[_rootNode].Id = -1;
        }

        ~FPTree()
        {
            Dispose(false);
        }

        public float Progress { get; private set; }

        public FrequentPatternMaxHeap[] Mine(int k, bool expandPerfectExtensions, int mineUntilItem, FrequentPatternMaxHeap[] itemHeaps)
        {
            FrequentPatternCollector collector = new FrequentPatternCollector(_headerTable.Length, _weight, expandPerfectExtensions, _minSupport, k, itemHeaps);

            _mineUntilItem = mineUntilItem;
            Mine(null, collector);

            // If we didn't mine the entire item set, those items we did mine can still have found patterns containing the lower items, so we need to report those as well.
            return collector.ItemHeaps;
        }

        public void PrintTree(TextWriter writer)
        {
            writer.WriteLine("Tree:");
            foreach (HeaderTableItem item in _headerTable)
            {
                int node = item.FirstNode;
                if (node != 0)
                {
                    while (node != 0)
                    {
                        int parentId = _nodes[_nodes[node].Parent].Id;
                        writer.Write("{0},{1}:{2}  |  ", parentId == -1 ? "*" : parentId.ToString(), _nodes[node].Id, _nodes[node].Count);
                        node = _nodes[node].NodeLink;
                    }
                    writer.WriteLine();
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Mine(int? currentItem, FrequentPatternCollector collector)
        {
            int lower = currentItem == null ? _mineUntilItem : 0;
            for (int x = _headerTable.Length - 1; x >= lower; --x)
            {
                MineItem(currentItem, collector, x);
                if (currentItem == null)
                {
                    int total = _headerTable.Length - lower;
                    int processed = _headerTable.Length - x;
                    Progress = processed / (float)total;
                    OnProgressChanged(EventArgs.Empty);
                }
            }
        }

        private void MineTopDown(int? currentItem, FrequentPatternCollector collector)
        {
            for (int x = 0; x < _headerTable.Length; ++x)
            {
                MineItem(currentItem, collector, x);
            }
        }

        private void MineItem(int? currentItem, FrequentPatternCollector collector, int item)
        {
            if (currentItem == null)
            {
                string message = string.Format("Mining for patterns with item id: {0}", item);
                _log.InfoFormat(message);
                _config.StatusMessage = message;
            }
            int minSupport = collector.GetMinSupportForItem(currentItem == null ? item : currentItem.Value);
            if (_headerTable[item].Support >= minSupport)
            {
                collector.Add(item, _headerTable[item].Support);
                int node = _headerTable[item].FirstNode;
                if (_nodes[node].NodeLink == 0)
                {
                    // Resulting tree would be single path.
                    for (node = _nodes[node].Parent; node > 0; node = _nodes[node].Parent)
                        collector.AddPerfectExtension(_nodes[node].Id);
                }
                else
                {
                    using (FPTree conditionalPatternTree = CreateConditionalTree(_headerTable[item].FirstNode))
                    {
                        conditionalPatternTree.ReduceTree(minSupport);
                        conditionalPatternTree.CollectPerfectItems(collector);
                        conditionalPatternTree.PruneTree(minSupport);
                        if (currentItem == null)
                            conditionalPatternTree.MineTopDown(item, collector);
                        else
                            conditionalPatternTree.Mine(currentItem, collector);
                    }
                }
                collector.Report();
                collector.Remove(1);
            }
        }


        private void BuildTree(IEnumerable<ITransaction> transactions)
        {
            int count = 0;

            foreach (ITransaction t in transactions)
            {
                if (++count % 10000 == 0)
                    _log.DebugFormat("Building tree: {0} transactions", count);

                IEnumerable<int> frequent = t.Items;

                int current = _rootNode;
                bool useExistingChild = true;
                foreach (int item in frequent)
                {
                    int child = useExistingChild ? GetChild(current, item) : 0;
                    if (child == 0)
                    {
                        useExistingChild = false; // no need to search for existing children after this.
                        child = CreateNode(current, item, t.Count);

                        _nodes[child].NodeLink = _headerTable[item].FirstNode;
                        _headerTable[item].FirstNode = child;
                    }
                    else
                        _nodes[child].Count += t.Count;
                    _headerTable[item].Support += t.Count;

                    current = child;
                }
            }

            _weight = count;
            _log.DebugFormat("Tree completed: {0} transactions", count);
        }

        private static int GetSupport(Dictionary<int, int> frequentItems, int item)
        {
            int support;
            if (frequentItems.TryGetValue(item, out support))
                return support;
            else
                return 0;
        }

        private FPTree CreateConditionalTree(int firstNode)
        {
            int conditionalNode = firstNode;
            FPTree conditionalTree = new FPTree(8, _nodes[conditionalNode].Id, _minSupport);

            while (conditionalNode != 0)
            {
                int pathNode = _nodes[conditionalNode].Parent;
                int previousPathNode = 0;
                int count = _nodes[conditionalNode].Count;

                while (pathNode != _rootNode)
                {
                    int id = _nodes[pathNode].Id;
                    if (_nodes[pathNode].Copy == 0)
                    {
                        int copy = conditionalTree.CreateConditionalNode(id, count);
                        _nodes[pathNode].Copy = copy;
                    }
                    else
                    {
                        conditionalTree.AddHeaderSupport(id, count);
                        conditionalTree.AddNodeSupport(_nodes[pathNode].Copy, count);
                    }

                    if (previousPathNode != 0)
                        conditionalTree.SetParent(previousPathNode, _nodes[pathNode].Copy);

                    previousPathNode = _nodes[pathNode].Copy;
                    pathNode = _nodes[pathNode].Parent;
                }

                conditionalNode = _nodes[conditionalNode].NodeLink;
            }

            conditionalNode = firstNode;
            while (conditionalNode != 0)
            {
                int pathNode = _nodes[conditionalNode].Parent;
                while (pathNode != 0)
                {
                    _nodes[pathNode].Copy = 0;

                    pathNode = _nodes[pathNode].Parent;
                }

                conditionalNode = _nodes[conditionalNode].NodeLink;
            }

            return conditionalTree;
        }

        private void ReduceTree(int minSupport)
        {
            int count = _headerTable.Length;
            while (count > 0 && _headerTable[count - 1].Support < minSupport)
            {
                // The children collection isn't used outside of tree construction, and it won't even be set in a conditional tree
                // so no need to remove the deleted nodes from the children collection of their parents.
                _headerTable[--count].Support = 0;
                _headerTable[count].FirstNode = 0;
            }
        }

        private void PruneTree(int minSupport)
        {
            int first;
            int count = _headerTable.Length;
            for (first = 0; first < count; ++first)
            {
                if (_headerTable[first].FirstNode != 0 && _headerTable[first].Support < minSupport)
                    break;
            }

            for (int i = first; i < count; ++i)
            {
                if (_headerTable[i].Support < minSupport)
                    continue; // skip levels with infrequent items

                for (int node = _headerTable[i].FirstNode; node != 0; node = _nodes[node].NodeLink)
                {
                    int ancestor = _nodes[node].Parent;
                    while (ancestor > 0 && _headerTable[_nodes[ancestor].Id].Support < minSupport)
                        ancestor = _nodes[ancestor].Parent;
                    if (_nodes[ancestor].Copy != 0 && _nodes[_nodes[ancestor].Copy].Id == _nodes[ancestor].Id)
                        ancestor = _nodes[ancestor].Copy;
                    if (_nodes[ancestor].Copy == 0 || _nodes[_nodes[ancestor].Copy].Id != i)
                    {
                        _nodes[ancestor].Copy = node;
                        _nodes[node].Parent = ancestor;
                    }
                    else
                    {
                        _nodes[node].Copy = _nodes[ancestor].Copy;
                        _nodes[_nodes[node].Copy].Count += _nodes[node].Count;
                    }
                }
            }
            for (; first < count; ++first)
            {
                int node = _headerTable[first].FirstNode;
                if (node == 0)
                    continue;
                if (_headerTable[first].Support < minSupport)
                {
                    _headerTable[first].Support = 0;
                    _headerTable[first].FirstNode = 0;
                }
                else
                {
                    while (_nodes[node].NodeLink != 0)
                    {
                        if (_nodes[_nodes[node].NodeLink].Copy == 0 || _nodes[_nodes[_nodes[node].NodeLink].Copy].Id != first)
                        {
                            _nodes[_nodes[node].Parent].Copy = _nodes[node].Copy = 0;
                            node = _nodes[node].NodeLink;
                        }
                        else
                        {
                            _nodes[node].NodeLink = _nodes[_nodes[node].NodeLink].NodeLink;
                        }
                    }
                    _nodes[_nodes[node].Parent].Copy = _nodes[node].Copy = 0;
                }
            }
        }

        private void CollectPerfectItems(FrequentPatternCollector collector)
        {
            //int i, r = 0;                 /* loop variable, counter */
            //int min;                      /* minimum support for perf. exts. */

            //assert(fpt && rd);            /* check the function arguments */



            //min = isr_supp(rd->isrep);    /* minimum for perfect extension */
            int min = collector.Support;
            int r = 0;
            for (int i = _headerTable.Length - 1; i >= 0; --i)
            {
                if (_headerTable[i].Support < min)
                    continue;
                collector.AddPerfectExtension(i);
                _headerTable[i].Support = 0;
                ++r;
            }
            if (r <= 0)
                return;
            ReduceTree(1);
            PruneTree(1);
        }

        private int GetChild(int node, int id)
        {
            int childCount = _nodeChildren[node].Count;
            if (childCount > 0)
            {
                int* children = _nodeChildren[node].Children;
                for (int x = 0; x < childCount; ++x)
                {
                    int child = children[x];
                    if (_nodes[child].Id == id)
                        return child;
                }

            }
            return 0;
        }

        private int CreateNode(int parentNode, int id, int count)
        {
            if (_nodeCount == _nodesLength)
                Resize();

            int newNode = _nodeCount++;
            _nodes[newNode] = new FPTreeNode();
            _nodes[newNode].Id = id;
            _nodes[newNode].Count = count;
            _nodes[newNode].Parent = parentNode;
            _nodeChildren[newNode] = new NodeChildList();
            _nodeChildren[parentNode].Add(newNode);

            return newNode;

        }

        private int CreateConditionalNode(int id, int count)
        {
            if (_nodeCount == _nodesLength)
                Resize();

            int newNode = _nodeCount++;
            _nodes[newNode] = new FPTreeNode();
            _nodes[newNode].Id = id;
            _nodes[newNode].Count = count;
            _nodes[newNode].NodeLink = _headerTable[id].FirstNode;
            _headerTable[id].FirstNode = newNode;
            _headerTable[id].Support += count;

            return newNode;
        }

        private void AddHeaderSupport(int id, int support)
        {
            _headerTable[id].Support += support;
        }

        private void AddNodeSupport(int node, int support)
        {
            _nodes[node].Count += support;
        }

        private void SetParent(int node, int parentNode)
        {
            _nodes[node].Parent = parentNode;
        }

        private void Resize()
        {
            _nodesLength = (int)(_nodesLength * _growthRate);
            _nodes = (FPTreeNode*)Marshal.ReAllocHGlobal((IntPtr)_nodes, new IntPtr(_nodesLength * sizeof(FPTreeNode)));
            if (_nodeChildren != null)
                _nodeChildren = (NodeChildList*)Marshal.ReAllocHGlobal((IntPtr)_nodeChildren, new IntPtr(_nodesLength * sizeof(NodeChildList)));
        }

        private void OnProgressChanged(EventArgs e)
        {
            EventHandler handler = ProgressChanged;
            if (handler != null)
                handler(this, e);
        }

        private void CleanupChildren()
        {
            if (_nodeChildren != null)
            {
                for (int x = 0; x < _nodeCount; ++x)
                {
                    if (_nodeChildren[x].Children != null)
                        Marshal.FreeHGlobal((IntPtr)_nodeChildren[x].Children);
                }
                Marshal.FreeHGlobal((IntPtr)_nodeChildren);
                _nodeChildren = null;
            }
        }

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true;
                CleanupChildren();
                if (_nodes != null)
                {
                    Marshal.FreeHGlobal((IntPtr)_nodes);
                    _nodes = null;
                }
            }
        }

    }
}
