// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo.IO;
using Ookii.Jumbo;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    class FrequentPatternCollector
    {

        private int _count;
        //private int _prefixCount;
        private readonly int[] _items;
        private readonly int[] _supports;
        // The high bit in _perfectExtensionCount is also used to indicate whether an item is present or not.
        private readonly int[] _perfectExtensionCount;
        private readonly int[] _perfectExtensions;
        private int _perfectExtensionItemIndex;
        private readonly FrequentPatternMaxHeap[] _itemHeaps;
        private readonly int _minSupport;
        private readonly bool _expandPerfectExtensions;
        private readonly int _heapSize;

        public FrequentPatternCollector(int itemCount, int weight, bool expandPerfectExtensions, int minSupport, int k, FrequentPatternMaxHeap[] itemHeaps)
        {
            _items = new int[itemCount];
            _perfectExtensions = new int[itemCount];
            _supports = new int[itemCount + 1];
            _perfectExtensionCount = new int[itemCount + 1];
            _supports[0] = weight;
            _minSupport = minSupport;
            _expandPerfectExtensions = expandPerfectExtensions;

            if( k > 0 )
            {
                _heapSize = k;
                if( itemHeaps == null || itemHeaps.Length < itemCount )
                {
                    _itemHeaps = new FrequentPatternMaxHeap[itemCount];
                    if( itemHeaps != null )
                        Array.Copy(itemHeaps, _itemHeaps, itemHeaps.Length);
                }
                else
                    _itemHeaps = itemHeaps;
            }
        }

        public int Support
        {
            get { return _supports[_count]; }
        }

        public int GetMinSupportForItem(int item)
        {
            return (_itemHeaps == null || _itemHeaps[item] == null) ? _minSupport : _itemHeaps[item].MinSupport;
        }

        public FrequentPatternMaxHeap[] ItemHeaps
        {
            get { return _itemHeaps; }
        }

        public void Add(int item, int support)
        {
            if( _perfectExtensionCount[item] < 0 )
                throw new InvalidOperationException("Duplicate item.");

            _perfectExtensionCount[item] |= Int32.MinValue; // mark the item used.
            _items[_count] = item;
            ++_count;
            _supports[_count] = support;
            _perfectExtensionCount[_count] &= Int32.MinValue; // Clear the perfect extension count for this prefix.
        }

        public void AddPerfectExtension(int item)
        {
            if( _perfectExtensionCount[item] < 0 )
                throw new InvalidOperationException("Duplicate item.");

            _perfectExtensionCount[item] |= Int32.MinValue; // mark the item used.
            _perfectExtensions[_perfectExtensionItemIndex] = item;
            ++_perfectExtensionItemIndex;
            _perfectExtensionCount[_count]++; // count it for the current prefix.
        }

        public void Remove(int count)
        {
            if( count > _count )
                count = _count;
            while( --count >= 0 )
            {
                // Remove the perfect extensions by clearing their used bit and decrementing the perfect exntension index.
                for( int i = _perfectExtensionCount[_count] & ~Int32.MinValue; --i >= 0; )
                    _perfectExtensionCount[_perfectExtensions[--_perfectExtensionItemIndex]] &= ~Int32.MinValue;
                int item = _items[--_count];
                _perfectExtensionCount[item] &= ~Int32.MinValue; // Clear the item's used bit.
            }
            //if( _count < _prefixCount )
            //    _prefixCount = _count;
        }

        public void Report()
        {
            if( _perfectExtensionItemIndex > 0 )
                ReportPerfectExtensions(0);

            if( _perfectExtensionItemIndex == 0 || _expandPerfectExtensions )
                Output();
        }

        private void ReportPerfectExtensions(int index)
        {
            if( _expandPerfectExtensions )
            {
                do
                {
                    _items[_count++] = _perfectExtensions[index++];
                    _supports[_count] = _supports[_count - 1];
                    if( index < _perfectExtensionItemIndex )
                        ReportPerfectExtensions(index);
                    Output();
                    --_count;
                    //if( _count < _prefixCount )
                    //    _prefixCount = _count;
                } while( index < _perfectExtensionItemIndex );
            }
            else
            {
                // index is always 0 here if we do this.
                do
                {
                    _items[_count++] = _perfectExtensions[index++];
                    _supports[_count] = _supports[_count - 1];
                } while( index < _perfectExtensionItemIndex );
                Output();
                _count -= index;
            }
        }

        private void Output()
        {
            if( _itemHeaps == null )
            {
                throw new NotSupportedException();
                //MappedFrequentPattern pattern = new MappedFrequentPattern(_items.Take(_count), _supports[_count]);
                //_output.WriteRecord(pattern);
            }
            else
            {
                // Don't do the reverse mapping yet, just keep using the IDs.
                MappedFrequentPattern pattern = new MappedFrequentPattern(_items.Take(_count).OrderByDescending(x => x).ToArray(), _supports[_count]);
                //FrequentPattern<int> pattern = new FrequentPattern<int>() { Items = _items.Take(_count).ToArray(), Support = _supports[_count] };
                //_itemHeaps[pattern.Items[0]].Add(pattern);
                foreach( int item in pattern.Items )
                {
                    if( _itemHeaps[item] == null )
                        _itemHeaps[item] = new FrequentPatternMaxHeap(_heapSize, _minSupport, true);
                    _itemHeaps[item].Add(pattern);
                }
            }
        }
    }
}
