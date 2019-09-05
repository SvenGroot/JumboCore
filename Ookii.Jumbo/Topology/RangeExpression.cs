// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Ookii.Jumbo.Topology
{
    /// <summary>
    /// Provides simple numeric range-based string matching.
    /// </summary>
    /// <remarks>
    /// <para>
    ///   The <see cref="RangeExpression"/> class allows you to match strings containing ranges of numbers.
    /// </para>
    /// <para>
    ///   String matching is done using a pattern. This pattern, in addition to regular text, can contain
    ///   numeric ranges of the form "[n-m]" where n and m are both numbers, e.g. "[001-299]". The ranges
    ///   are inclusive.
    /// </para>
    /// <para>
    ///   The number of characters in the lower bound of the range determines the minimum number of characters;
    ///   e.g. the expression "foo[001-299]" will match "foo001" but not "foo1", whereas the expression "foo[1-299]"
    ///   would match either.
    /// </para>
    /// <para>
    ///   The number of characters in the upper bound determines the maximum number of characters; e.g.
    ///   the expression "foo[1-099] matches "foo050" and "foo50" but not "foo0050".
    /// </para>
    /// <para>
    ///   In addition to ranges, you can also use alternation with the | character. For example, "foo[00-50]|bar[51-99]"
    ///   will match both e.g. "foo25" and "bar75".
    /// </para>
    /// <para>
    ///   You can also group subexpressions in parentheses for use with alternation. For example "(foo|bar)[00-50]" will
    ///   match both e.g. "foo25" and "bar25".
    /// </para>
    /// </remarks>
    public sealed class RangeExpression
    {
        #region Nested types

        private enum ParseState
        {
            Text,
            EscapedText,
            RangeMinimum,
            RangeMaximum
        }

        private abstract class BaseNode
        {
            public abstract int Match(string value, int index, bool matchCase);
        }

        private sealed class TextNode : BaseNode
        {
            private readonly string _text;

            public TextNode(string text)
            {
                if( text == null )
                    throw new ArgumentNullException("text");

                _text = text;
            }

            public override int Match(string value, int index, bool matchCase)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
                // We want to return -1 if index == length! This is caught by the end > length comparison below.
                if( index < 0 || index > value.Length )
                    throw new ArgumentOutOfRangeException("index");

                int end = index + _text.Length;
                if( end > value.Length )
                    return -1;

                for( int matchIndex = 0; index < end; ++index, ++matchIndex )
                {
                    if( matchCase ? value[index] != _text[matchIndex] : char.ToUpperInvariant(value[index]) != char.ToUpperInvariant(_text[matchIndex]) )
                        return -1;
                }

                return index;
            }

            public override string ToString()
            {
                return _text;
            }
        }

        private sealed class RangeNode : BaseNode
        {
            private readonly int _minInclusive;
            private readonly int _maxInclusive;
            private readonly int _minCharCount;
            private readonly int _maxCharCount;

            public RangeNode(int minInclusive, int maxInclusive, int minCharCount, int maxCharCount)
            {
                if( minInclusive < 0 )
                    throw new ArgumentOutOfRangeException("minInclusive");
                if( maxInclusive < minInclusive )
                    throw new ArgumentOutOfRangeException("maxInclusive");
                if( minCharCount < 1 )
                    throw new ArgumentOutOfRangeException("minCharCount");
                if( maxCharCount < minCharCount )
                    throw new ArgumentOutOfRangeException("maxCharCount");

                _minInclusive = minInclusive;
                _maxInclusive = maxInclusive;
                _minCharCount = minCharCount;
                _maxCharCount = maxCharCount;
            }

            public override int Match(string value, int index, bool matchCase)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
                // We want to return -1 if index == length. This is caught by the loop and min char count check below.
                if( index < 0 || index > value.Length )
                    throw new ArgumentOutOfRangeException("index");

                int start = index;
                while( index < value.Length && char.IsNumber(value, index) )
                {
                    ++index;
                }

                int count = index - start;
                if( count >= _minCharCount && count <= _maxCharCount )
                {
                    int number;
                    if( int.TryParse(value.Substring(start, index - start), out number) )
                    {
                        if( number >= _minInclusive && number <= _maxInclusive )
                            return index;
                    }
                }
                return -1;
            }

            public override string ToString()
            {
                return string.Format(CultureInfo.InvariantCulture, "[{0}-{1}]", _minInclusive.ToString(CultureInfo.InvariantCulture).PadLeft(_minCharCount, '0'), _maxInclusive.ToString(CultureInfo.InvariantCulture).PadLeft(_maxCharCount, '0'));
            }
        }

        private sealed class ChoiceNode : BaseNode
        {
            private readonly List<List<BaseNode>> _choices = new List<List<BaseNode>>();

            public void AddChoice(List<BaseNode> choice)
            {
                if( choice == null )
                    throw new ArgumentNullException("choice");

                _choices.Add(choice);
            }

            public List<BaseNode> LastChoice
            {
                get { return _choices[_choices.Count - 1]; }
            }

            public override int Match(string value, int index, bool matchCase)
            {
                if( value == null )
                    throw new ArgumentNullException("value");
                if( index < 0 || index > value.Length )
                    throw new ArgumentOutOfRangeException("index");

                foreach( List<BaseNode> choice in _choices )
                {
                    int current = index;

                    foreach( BaseNode node in choice )
                    {
                        current = node.Match(value, current, matchCase);
                        if( current < 0 )
                            break;
                    }
                    if( current >= 0 )
                        return current;
                }

                return -1;
            }

            public override string ToString()
            {
                StringBuilder result = new StringBuilder();
                bool first = true;
                foreach( List<BaseNode> choice in _choices )
                {
                    if( first )
                        first = false;
                    else
                        result.Append('|');

                    foreach( BaseNode node in choice )
                    {
                        if( node is ChoiceNode && choice.Count > 1 )
                        {
                            result.Append('(');
                            result.Append(node);
                            result.Append(')');
                        }
                        else
                            result.Append(node);
                    }
                }

                return result.ToString();
            }
        }

        #endregion

        private readonly List<BaseNode> _nodes;

        /// <summary>
        /// Initializes a new instance of the <see cref="RangeExpression"/> class.
        /// </summary>
        /// <param name="pattern">The pattern to match against..</param>
        /// <exception cref="ArgumentNullException"><paramref name="pattern"/> is <see langword="null"/>.</exception>
        /// <exception cref="FormatException"><paramref name="pattern"/> is not a valid range expression.</exception>
        public RangeExpression(string pattern)
        {
            if( pattern == null )
                throw new ArgumentNullException("pattern");

            _nodes = ParsePattern(pattern);
        }

        /// <summary>
        /// Matches the specified string against the pattern.
        /// </summary>
        /// <param name="value">The string to match.</param>
        /// <returns><see langword="true"/> if <paramref name="value"/> matches the pattern; otherwise, <see langword="false"/>.</returns>
        public bool Match(string value)
        {
            return Match(value, true);
        }

        /// <summary>
        /// Matches the specified string against the pattern.
        /// </summary>
        /// <param name="value">The string to match.</param>
        /// <param name="matchCase"><see langword="true"/> to perform a case-sensitive comparison; <see langword="false"/> to perform a case-insensitive comparison.</param>
        /// <returns><see langword="true"/> if <paramref name="value"/> matches the pattern; otherwise, <see langword="false"/>.</returns>
        public bool Match(string value, bool matchCase)
        {
            if( value == null )
                throw new ArgumentNullException("value");

            int index = 0;
            foreach( BaseNode node in _nodes )
            {
                index = node.Match(value, index, matchCase);
                if( index < 0 )
                    return false;
            }

            return index == value.Length;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity")]
        private static List<BaseNode> ParsePattern(string pattern)
        {
            List<BaseNode> current = new List<BaseNode>();
            List<BaseNode> currentGroup = current;
            Stack<Tuple<List<BaseNode>, ChoiceNode>> groups = null;
            ChoiceNode choice = null;
            StringBuilder temp = new StringBuilder(pattern.Length);
            ParseState state = ParseState.Text;
            int minInclusive = 0;
            int minCharCount = 0;

            foreach( char c in pattern )
            {
                switch( state )
                {
                case ParseState.Text:
                    switch( c )
                    {
                    case '[':
                        AddCurrentTextNode(current, temp);
                        temp.Length = 0;
                        state = ParseState.RangeMinimum;
                        break;
                    case '\\':
                        state = ParseState.EscapedText;
                        break;
                    case '(':
                        AddCurrentTextNode(current, temp);
                        if( groups == null )
                            groups = new Stack<Tuple<List<BaseNode>, ChoiceNode>>();
                        groups.Push(Tuple.Create(currentGroup, choice));
                        currentGroup = new List<BaseNode>();
                        current = currentGroup;
                        choice = null;
                        break;
                    case ')':
                        AddCurrentTextNode(current, temp);
                        if( groups == null || groups.Count == 0 )
                            throw new FormatException("Invalid range expression.");
                        var parent = groups.Pop();
                        List<BaseNode> group = currentGroup;
                        currentGroup = parent.Item1;
                        choice = parent.Item2;
                        if( choice == null )
                            current = currentGroup;
                        else
                            current = choice.LastChoice;

                        // Groups are only important for choice expressions. If the group contains a choice expression, it contains
                        // only one node, if not it served no purpose, so either way we just add it to the parent.
                        current.AddRange(group);
                        break;
                    case '|':
                        AddCurrentTextNode(current, temp);
                        if( choice == null )
                        {
                            choice = new ChoiceNode();
                            choice.AddChoice(current);
                            currentGroup = new List<BaseNode>() { choice };
                        }
                        current = new List<BaseNode>();
                        choice.AddChoice(current);
                        break;
                    default:
                        temp.Append(c);
                        break;
                    }
                    break;
                case ParseState.EscapedText:
                    temp.Append(c);
                    state = ParseState.Text;
                    break;
                case ParseState.RangeMinimum:
                    if( char.IsNumber(c) )
                        temp.Append(c);
                    else if( c == '-' )
                    {
                        if( temp.Length == 0 )
                            throw new FormatException("Invalid range expression.");
                        minInclusive = int.Parse(temp.ToString(), CultureInfo.InvariantCulture);
                        minCharCount = temp.Length;
                        temp.Length = 0;
                        state = ParseState.RangeMaximum;
                    }
                    else
                        throw new FormatException("Invalid range expression.");
                    break;
                case ParseState.RangeMaximum:
                    if( char.IsNumber(c) )
                        temp.Append(c);
                    else if( c == ']' )
                    {
                        if( temp.Length == 0 )
                            throw new FormatException("Invalid range expression.");
                        current.Add(new RangeNode(minInclusive, int.Parse(temp.ToString(), CultureInfo.InvariantCulture), minCharCount, Math.Max(minCharCount, temp.Length)));
                        temp.Length = 0;
                        state = ParseState.Text;
                    }
                    else
                        throw new FormatException("Invalid range expression.");
                    break;
                }
            }

            if( state != ParseState.Text || groups != null && groups.Count > 0 )
                throw new FormatException("Invalid range expression.");

            if( temp.Length > 0 )
                current.Add(new TextNode(temp.ToString()));

            return currentGroup;
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            StringBuilder result = new StringBuilder();
            foreach( BaseNode node in _nodes )
            {
                if( node is ChoiceNode && _nodes.Count > 1 )
                {
                    result.Append("(");
                    result.Append(node);
                    result.Append(")");
                }
                else
                    result.Append(node);
            }

            return result.ToString();
        }

        private static void AddCurrentTextNode(List<BaseNode> current, StringBuilder temp)
        {
            if( temp.Length > 0 )
            {
                current.Add(new TextNode(temp.ToString()));
                temp.Length = 0;
            }
        }
    }
}
