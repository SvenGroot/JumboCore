using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Ookii.Jumbo.Topology;

namespace Ookii.Jumbo.Test
{
    [TestFixture]
    public class RangeExpressionTests
    {
        [Test]
        public void TestTextMatching()
        {
            RangeExpression target = new RangeExpression("foo");
            Assert.IsTrue(target.Match("foo"));
            Assert.IsFalse(target.Match("bar"));
            Assert.IsFalse(target.Match("fo"));
            Assert.IsFalse(target.Match("foo0"));
            Assert.IsFalse(target.Match("0foo"));
            Assert.IsFalse(target.Match("FOO"));
            Assert.IsTrue(target.Match("FOO", false));
        }

        [Test]
        public void TestRangeMatching()
        {
            RangeExpression target = new RangeExpression("[010-0123]");
            Assert.IsTrue(target.Match("010"));
            Assert.IsTrue(target.Match("123"));
            Assert.IsTrue(target.Match("010"));
            Assert.IsTrue(target.Match("0123"));
            Assert.IsTrue(target.Match("0010"));
            Assert.IsFalse(target.Match("10"));
            Assert.IsFalse(target.Match("00010"));
            Assert.IsFalse(target.Match("009"));
            Assert.IsFalse(target.Match("124"));
        }

        [Test]
        public void TestTextAndRangeMatching()
        {
            RangeExpression target = new RangeExpression("foo[010-0123]bar");
            Assert.IsTrue(target.Match("foo010bar"));
            Assert.IsTrue(target.Match("foo123bar"));
            Assert.IsTrue(target.Match("foo010bar"));
            Assert.IsTrue(target.Match("foo0123bar"));
            Assert.IsTrue(target.Match("foo0010bar"));
            Assert.IsFalse(target.Match("foo10bar"));
            Assert.IsFalse(target.Match("foo00010bar"));
            Assert.IsFalse(target.Match("foo009bar"));
            Assert.IsFalse(target.Match("foo124bar"));
            Assert.IsFalse(target.Match("FOO010BAR"));
            Assert.IsTrue(target.Match("FoO010BaR", false));
            Assert.IsFalse(target.Match("foo010"));
            Assert.IsFalse(target.Match("010"));
            Assert.IsFalse(target.Match("foobar"));
            Assert.IsFalse(target.Match("foo"));
        }

        [Test]
        public void TestAlternation()
        {
            RangeExpression target = new RangeExpression("foo[00-50]|[51-100]bar");
            Assert.IsTrue(target.Match("foo00"));
            Assert.IsTrue(target.Match("51bar"));
            Assert.IsFalse(target.Match("foo51"));
            Assert.IsFalse(target.Match("00bar"));
            Assert.IsFalse(target.Match("foo0051bar"));
        }

        [Test]
        public void TestGrouping()
        {
            RangeExpression target = new RangeExpression("foo(bar|[0-9]test)[00-50]");
            Assert.IsTrue(target.Match("foobar00"));
            Assert.IsTrue(target.Match("foo9test50"));
            Assert.IsFalse(target.Match("foo00"));
            Assert.IsFalse(target.Match("foobar"));
            Assert.IsFalse(target.Match("footest00"));
            Assert.IsFalse(target.Match("foo5test25b"));
        }
    }
}
