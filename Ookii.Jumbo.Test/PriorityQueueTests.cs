// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class PriorityQueueTests
    {
        /// <summary>
        /// Used for AdjustFirstItem test.
        /// </summary>
        private class QueueItem : IComparable<QueueItem>
        {
            public QueueItem(string value)
            {
                Value = value;
            }

            public string Value { get; set; }

            #region IComparable<QueueItem> Members

            public int CompareTo(QueueItem other)
            {
                return string.Compare(Value, other.Value);
            }

            #endregion
        }

        [Test]
        public void TestEnqueueDequeue()
        {
            PriorityQueue<string> queue = new PriorityQueue<string>();

            Assert.AreEqual(0, queue.Count);

            queue.Enqueue("d");
            Assert.AreEqual("d", queue.Peek());
            Assert.AreEqual(1, queue.Count);

            queue.Enqueue("c");
            Assert.AreEqual("c", queue.Peek());
            Assert.AreEqual(2, queue.Count);

            queue.Enqueue("a");
            Assert.AreEqual("a", queue.Peek());
            Assert.AreEqual(3, queue.Count);

            queue.Enqueue("b");
            Assert.AreEqual("a", queue.Peek());
            Assert.AreEqual(4, queue.Count);

            queue.Enqueue("c");
            Assert.AreEqual("a", queue.Peek());
            Assert.AreEqual(5, queue.Count);

            string item = queue.Dequeue();
            Assert.AreEqual("a", item);
            Assert.AreEqual(4, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("b", item);
            Assert.AreEqual(3, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item);
            Assert.AreEqual(2, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("c", item);
            Assert.AreEqual(1, queue.Count);

            item = queue.Dequeue();
            Assert.AreEqual("d", item);
            Assert.AreEqual(0, queue.Count);
        }

        [Test]
        public void TestAdjustFirstItem()
        {
            PriorityQueue<QueueItem> target = new PriorityQueue<QueueItem>();

            target.Enqueue(new QueueItem("b"));
            target.Enqueue(new QueueItem("d"));
            target.Enqueue(new QueueItem("f"));
            target.Enqueue(new QueueItem("h"));

            Assert.AreEqual("b", target.Peek().Value);
            Assert.AreEqual(4, target.Count);
            
            // changing b to c
            target.Peek().Value = "c";
            target.AdjustFirstItem();
            Assert.AreEqual("c", target.Peek().Value);
            Assert.AreEqual(4, target.Count);

            // changing c to e
            target.Peek().Value = "e";
            target.AdjustFirstItem();
            Assert.AreEqual("d", target.Peek().Value);
            Assert.AreEqual(4, target.Count);

            // changing d to g
            target.Peek().Value = "g";
            target.AdjustFirstItem();
            Assert.AreEqual("e", target.Peek().Value);
            Assert.AreEqual(4, target.Count);

            Assert.AreEqual("e", target.Dequeue().Value);
            Assert.AreEqual(3, target.Count);
            Assert.AreEqual("f", target.Dequeue().Value);
            Assert.AreEqual(2, target.Count);
            Assert.AreEqual("g", target.Dequeue().Value);
            Assert.AreEqual(1, target.Count);
            Assert.AreEqual("h", target.Dequeue().Value);
            Assert.AreEqual(0, target.Count);
        }
    }
}
