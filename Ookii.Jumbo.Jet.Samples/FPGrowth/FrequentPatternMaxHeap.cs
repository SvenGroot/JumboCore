// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ookii.Jumbo;
using Ookii.Jumbo.IO;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    sealed class FrequentPatternMaxHeap
    {
        private readonly PriorityQueue<MappedFrequentPattern> _queue;
        private readonly int _maxSize;
        private int _minSupport;
        private Dictionary<int, HashSet<MappedFrequentPattern>> _patternIndex;
        private readonly bool _subPatternCheck;
        private int _addCount;

        public FrequentPatternMaxHeap(int maxSize, int minSupport, bool subPatternCheck, IEnumerable<MappedFrequentPattern> collection)
        {
            _minSupport = minSupport;
            _maxSize = maxSize;
            if( collection == null )
                _queue = new PriorityQueue<MappedFrequentPattern>(maxSize + 1, null);
            else
            {
                _queue = new PriorityQueue<MappedFrequentPattern>(collection);
                if( _queue.Capacity < maxSize )
                    _queue.Capacity = maxSize + 1;
            }
            _subPatternCheck = subPatternCheck;
            if( subPatternCheck )
            {
                _patternIndex = new Dictionary<int, HashSet<MappedFrequentPattern>>();
                if( collection != null )
                {
                    foreach( MappedFrequentPattern pattern in collection )
                    {
                        HashSet<MappedFrequentPattern> index;
                        if( !_patternIndex.TryGetValue(pattern.Support, out index) )
                        {
                            index = new HashSet<MappedFrequentPattern>();
                            _patternIndex.Add(pattern.Support, index);
                        }
                        index.Add(pattern);
                    }
                }
            }
        }

        public FrequentPatternMaxHeap(int maxSize, int minSupport, bool subPatternCheck)
            : this(maxSize, minSupport, subPatternCheck, null)
        {
        }

        public int MinSupport
        {
            get { return _minSupport; }
        }

        public PriorityQueue<MappedFrequentPattern> Queue
        {
            get 
            {
                if( _subPatternCheck )
                {
                    PriorityQueue<MappedFrequentPattern> result = new PriorityQueue<MappedFrequentPattern>(_maxSize, null);
                    foreach( MappedFrequentPattern p in _queue )
                    {
                        if( _patternIndex[p.Support].Contains(p) )
                            result.Enqueue(p);
                    }
                    return result;
                }
                return _queue; 
            }
        }


        public void Add(MappedFrequentPattern pattern)
        {
            if( _queue.Count == _maxSize )
            {
                if( pattern.CompareTo(_queue.Peek()) > 0 && AddInternal(pattern) )
                {
                    MappedFrequentPattern removedPattern = _queue.Dequeue();
                    if( _subPatternCheck )
                        _patternIndex[removedPattern.Support].Remove(removedPattern);
                    _minSupport = _queue.Peek().Support;
                }
            }
            else
            {
                if( AddInternal(pattern) )
                {
                    _minSupport = Math.Min(_minSupport, pattern.Support);
                }
            }
        }

        public void OutputItems(int item, RecordWriter<Pair<int, WritableCollection<MappedFrequentPattern>>> output)
        {
            WritableCollection<MappedFrequentPattern> patterns = new WritableCollection<MappedFrequentPattern>();
            PriorityQueue<MappedFrequentPattern> queue = Queue;
            //_log.InfoFormat("{2}: Found {0} frequent items with min support {1}.", queue.Count, queue.Peek().Support, item);
            while( queue.Count > 0 )
            {
                patterns.Add(queue.Dequeue());
            }

            output.WriteRecord(Pair.MakePair(item, patterns));
        }

        public void OutputItems(int item, RecordWriter<Pair<int, MappedFrequentPattern>> output)
        {
            PriorityQueue<MappedFrequentPattern> queue = Queue;
            //_log.InfoFormat("{2}: Found {0} frequent items with min support {1}.", queue.Count, queue.Peek().Support, item);
            Pair<int, MappedFrequentPattern> record = new Pair<int,MappedFrequentPattern>();
            record.Key = item;
            while( queue.Count > 0 )
            {
                record.Value = queue.Dequeue();
                output.WriteRecord(record);
            }
        }

        private bool AddInternal(MappedFrequentPattern pattern)
        {
            ++_addCount;
            if( !_subPatternCheck )
            {
                _queue.Enqueue(pattern);
                return true;
            }
            else
            {
                HashSet<MappedFrequentPattern> index;
                if( _patternIndex.TryGetValue(pattern.Support, out index) )
                {
                    MappedFrequentPattern patternToReplace = null;
                    foreach( MappedFrequentPattern p in index )
                    {
                        if( pattern.IsSubpatternOf(p) )
                            return false;
                        else if( p.IsSubpatternOf(pattern) )
                        {
                            patternToReplace = p;
                            break;
                        }
                    }

                    if( patternToReplace != null )
                    {
                        index.Remove(patternToReplace);
                        _queue.Remove(patternToReplace);
                        if( !index.Contains(pattern) )
                        {
                            _queue.Enqueue(pattern);
                            index.Add(pattern);
                        }
                        return false;
                    }
                }
                else
                {
                    index = new HashSet<MappedFrequentPattern>();
                    _patternIndex.Add(pattern.Support, index);
                }

                _queue.Enqueue(pattern);
                index.Add(pattern);
                return true;
            }
        }

    }
}
