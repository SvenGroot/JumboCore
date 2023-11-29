using NUnit.Framework;
using Ookii.Jumbo.Topology;

namespace Ookii.Jumbo.Test;

[TestFixture]
public class RangeExpressionTests
{
    [Test]
    public void TestTextMatching()
    {
        RangeExpression target = new RangeExpression("foo");
        Assert.That(target.Match("foo"), Is.True);
        Assert.That(target.Match("bar"), Is.False);
        Assert.That(target.Match("fo"), Is.False);
        Assert.That(target.Match("foo0"), Is.False);
        Assert.That(target.Match("0foo"), Is.False);
        Assert.That(target.Match("FOO"), Is.False);
        Assert.That(target.Match("FOO", false), Is.True);
    }

    [Test]
    public void TestRangeMatching()
    {
        RangeExpression target = new RangeExpression("[010-0123]");
        Assert.That(target.Match("010"), Is.True);
        Assert.That(target.Match("123"), Is.True);
        Assert.That(target.Match("010"), Is.True);
        Assert.That(target.Match("0123"), Is.True);
        Assert.That(target.Match("0010"), Is.True);
        Assert.That(target.Match("10"), Is.False);
        Assert.That(target.Match("00010"), Is.False);
        Assert.That(target.Match("009"), Is.False);
        Assert.That(target.Match("124"), Is.False);
    }

    [Test]
    public void TestTextAndRangeMatching()
    {
        RangeExpression target = new RangeExpression("foo[010-0123]bar");
        Assert.That(target.Match("foo010bar"), Is.True);
        Assert.That(target.Match("foo123bar"), Is.True);
        Assert.That(target.Match("foo010bar"), Is.True);
        Assert.That(target.Match("foo0123bar"), Is.True);
        Assert.That(target.Match("foo0010bar"), Is.True);
        Assert.That(target.Match("foo10bar"), Is.False);
        Assert.That(target.Match("foo00010bar"), Is.False);
        Assert.That(target.Match("foo009bar"), Is.False);
        Assert.That(target.Match("foo124bar"), Is.False);
        Assert.That(target.Match("FOO010BAR"), Is.False);
        Assert.That(target.Match("FoO010BaR", false), Is.True);
        Assert.That(target.Match("foo010"), Is.False);
        Assert.That(target.Match("010"), Is.False);
        Assert.That(target.Match("foobar"), Is.False);
        Assert.That(target.Match("foo"), Is.False);
    }

    [Test]
    public void TestAlternation()
    {
        RangeExpression target = new RangeExpression("foo[00-50]|[51-100]bar");
        Assert.That(target.Match("foo00"), Is.True);
        Assert.That(target.Match("51bar"), Is.True);
        Assert.That(target.Match("foo51"), Is.False);
        Assert.That(target.Match("00bar"), Is.False);
        Assert.That(target.Match("foo0051bar"), Is.False);
    }

    [Test]
    public void TestGrouping()
    {
        RangeExpression target = new RangeExpression("foo(bar|[0-9]test)[00-50]");
        Assert.That(target.Match("foobar00"), Is.True);
        Assert.That(target.Match("foo9test50"), Is.True);
        Assert.That(target.Match("foo00"), Is.False);
        Assert.That(target.Match("foobar"), Is.False);
        Assert.That(target.Match("footest00"), Is.False);
        Assert.That(target.Match("foo5test25b"), Is.False);
    }
}
