// Copyright (c) Sven Groot (Ookii.org)
using System;
using NUnit.Framework;

namespace Ookii.Jumbo.Test;

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

        Assert.That(queue.Count, Is.EqualTo(0));

        queue.Enqueue("d");
        Assert.That(queue.Peek(), Is.EqualTo("d"));
        Assert.That(queue.Count, Is.EqualTo(1));

        queue.Enqueue("c");
        Assert.That(queue.Peek(), Is.EqualTo("c"));
        Assert.That(queue.Count, Is.EqualTo(2));

        queue.Enqueue("a");
        Assert.That(queue.Peek(), Is.EqualTo("a"));
        Assert.That(queue.Count, Is.EqualTo(3));

        queue.Enqueue("b");
        Assert.That(queue.Peek(), Is.EqualTo("a"));
        Assert.That(queue.Count, Is.EqualTo(4));

        queue.Enqueue("c");
        Assert.That(queue.Peek(), Is.EqualTo("a"));
        Assert.That(queue.Count, Is.EqualTo(5));

        string item = queue.Dequeue();
        Assert.That(item, Is.EqualTo("a"));
        Assert.That(queue.Count, Is.EqualTo(4));

        item = queue.Dequeue();
        Assert.That(item, Is.EqualTo("b"));
        Assert.That(queue.Count, Is.EqualTo(3));

        item = queue.Dequeue();
        Assert.That(item, Is.EqualTo("c"));
        Assert.That(queue.Count, Is.EqualTo(2));

        item = queue.Dequeue();
        Assert.That(item, Is.EqualTo("c"));
        Assert.That(queue.Count, Is.EqualTo(1));

        item = queue.Dequeue();
        Assert.That(item, Is.EqualTo("d"));
        Assert.That(queue.Count, Is.EqualTo(0));
    }

    [Test]
    public void TestAdjustFirstItem()
    {
        PriorityQueue<QueueItem> target = new PriorityQueue<QueueItem>();

        target.Enqueue(new QueueItem("b"));
        target.Enqueue(new QueueItem("d"));
        target.Enqueue(new QueueItem("f"));
        target.Enqueue(new QueueItem("h"));

        Assert.That(target.Peek().Value, Is.EqualTo("b"));
        Assert.That(target.Count, Is.EqualTo(4));

        // changing b to c
        target.Peek().Value = "c";
        target.AdjustFirstItem();
        Assert.That(target.Peek().Value, Is.EqualTo("c"));
        Assert.That(target.Count, Is.EqualTo(4));

        // changing c to e
        target.Peek().Value = "e";
        target.AdjustFirstItem();
        Assert.That(target.Peek().Value, Is.EqualTo("d"));
        Assert.That(target.Count, Is.EqualTo(4));

        // changing d to g
        target.Peek().Value = "g";
        target.AdjustFirstItem();
        Assert.That(target.Peek().Value, Is.EqualTo("e"));
        Assert.That(target.Count, Is.EqualTo(4));

        Assert.That(target.Dequeue().Value, Is.EqualTo("e"));
        Assert.That(target.Count, Is.EqualTo(3));
        Assert.That(target.Dequeue().Value, Is.EqualTo("f"));
        Assert.That(target.Count, Is.EqualTo(2));
        Assert.That(target.Dequeue().Value, Is.EqualTo("g"));
        Assert.That(target.Count, Is.EqualTo(1));
        Assert.That(target.Dequeue().Value, Is.EqualTo("h"));
        Assert.That(target.Count, Is.EqualTo(0));
    }
}
